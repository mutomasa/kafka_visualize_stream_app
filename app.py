import os
import time
import json
import random
from typing import Deque, List
from collections import deque

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from consumer import stream_events


def normalize_points(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["x", "y", "z", "value", "seq", "category"])
    df = raw.copy()
    # 既存列の確保とフォールバック
    if "value" not in df.columns:
        # t や i があれば簡易スケーリング
        if "t" in df.columns:
            df["value"] = pd.to_numeric(df["t"], errors="coerce").fillna(0)
        elif "i" in df.columns:
            df["value"] = pd.to_numeric(df["i"], errors="coerce").fillna(0)
        else:
            df["value"] = 0.0
    for col in ["x", "y", "z"]:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    if "seq" not in df.columns:
        df["seq"] = np.arange(len(df))
    if "category" not in df.columns:
        df["category"] = "N/A"
    # 型整備
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0.0)
    return df[["x", "y", "z", "value", "seq", "category"]]


def make_fig(points: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if not points.empty:
        fig.add_trace(
            go.Scatter3d(
                x=points["x"], y=points["y"], z=points["z"],
                mode="markers",
                marker=dict(
                    size=np.clip(points["value"].to_numpy()/10.0 + 3.0, 3, 15),
                    color=points["value"], colorscale="Viridis", opacity=0.8,
                ),
                text=[f"seq={int(s)} cat={c} v={v:.1f}" for s,c,v in zip(points["seq"], points["category"], points["value"])],
                hoverinfo="text",
            )
        )
    fig.update_layout(
        scene=dict(
            xaxis_title="X", yaxis_title="Y", zaxis_title="Z",
            aspectmode="cube",
        ),
        margin=dict(l=0, r=0, t=20, b=0),
        height=600,
    )
    return fig


def produce_test_messages(bootstrap: str, topic: str, n: int = 100) -> int:
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=10,
        )
        now = time.time()
        for i in range(n):
            producer.send(
                topic,
                {
                    "ts": time.time(),
                    "seq": i,
                    "category": random.choice(["A", "B", "C", "D"]),
                    "x": random.uniform(-10, 10),
                    "y": random.uniform(-10, 10),
                    "z": random.uniform(-10, 10),
                    "value": random.uniform(0, 100),
                },
            )
        producer.flush()
        return n
    except Exception:
        return 0


def main():
    st.set_page_config(page_title="Kafka 3D Stream", page_icon="📈", layout="wide")
    st.title("Kafka リアルタイム 3D ストリーミング可視化")

    with st.sidebar:
        st.header("設定")
        topic = st.text_input("Kafka Topic", os.environ.get("KAFKA_TOPIC", "stream3d"))
        bootstrap = st.text_input("Bootstrap Servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"))
        group_id = st.text_input(
            "Consumer Group",
            os.environ.get("KAFKA_GROUP", f"streamlit-viewer-{int(time.time())}")
        )
        offset_reset = st.selectbox("Offset Reset", ["earliest", "latest"], index=0)
        max_points = st.slider("表示する直近点数", min_value=100, max_value=5000, value=1000, step=100)
        poll_sec = st.slider("ポーリング間隔(秒)", min_value=0.05, max_value=2.0, value=0.2, step=0.05)
        run = st.checkbox("ストリーミング開始", value=False)
        if st.button("テストメッセージ投入 (100件)"):
            n = produce_test_messages(bootstrap, topic, 100)
            if n > 0:
                st.success(f"{n} 件のテストメッセージを投入しました")
            else:
                st.error("メッセージ投入に失敗しました（ブローカー/接続を確認してください）")

    # 環境変数に反映（consumer は参照）
    os.environ["KAFKA_TOPIC"] = topic
    os.environ["KAFKA_BOOTSTRAP"] = bootstrap
    os.environ["KAFKA_GROUP"] = group_id
    os.environ["KAFKA_OFFSET_RESET"] = offset_reset

    buffer: Deque[dict] = deque(maxlen=max_points)
    placeholder = st.empty()
    stats_ph = st.empty()
    # メトリクス用プレースホルダと状態
    col1, col2 = st.columns(2)
    rate_ph = col1.empty()
    total_ph = col2.empty()
    if "_total_received" not in st.session_state:
        st.session_state._total_received = 0
    if "_rate_ema" not in st.session_state:
        st.session_state._rate_ema = 0.0
    if "_plot_counter" not in st.session_state:
        st.session_state._plot_counter = 0

    if run:
        event_iter = stream_events()
        while True:
            start = time.time()
            # 取りこぼし防止のため、短時間で複数pull
            received_this_loop = 0
            for _ in range(5):
                try:
                    ev = next(event_iter)
                    buffer.append(ev)
                    received_this_loop += 1
                except StopIteration:
                    # タイムアウトしてイテレータが枯渇した場合は作り直す
                    event_iter = stream_events()
                    break
                except Exception:
                    break

            df_raw = pd.DataFrame(list(buffer)) if buffer else pd.DataFrame()
            df = normalize_points(df_raw)
            fig = make_fig(df)
            # 直前の要素を明示的にクリアしてから再描画し、ユニークキーを都度更新
            placeholder.empty()
            st.session_state._plot_counter += 1
            placeholder.plotly_chart(
                fig,
                use_container_width=True,
                key=f"stream_3d_{st.session_state._plot_counter}"
            )

            # このループの経過時間を先に計算
            elapsed = max(1e-6, time.time() - start)

            # ステータス表示
            stats_ph.info(f"受信バッファ: {len(buffer)} 件 | トピック: {topic} | ブローカー: {bootstrap}")

            # スループット計算と表示
            st.session_state._total_received += received_this_loop
            inst_rate = received_this_loop / elapsed
            alpha = 0.3
            st.session_state._rate_ema = alpha * inst_rate + (1 - alpha) * float(st.session_state._rate_ema)
            rate_ph.metric("Throughput (msg/s)", f"{inst_rate:.2f}", delta=f"EMA {st.session_state._rate_ema:.2f}")
            total_ph.metric("Total received", f"{st.session_state._total_received}")

            time.sleep(max(0.0, poll_sec - elapsed))
    else:
        st.info("左のサイドバーで Kafka 接続情報を設定し、ストリーミング開始を有効化してください。")


if __name__ == "__main__":
    main()


