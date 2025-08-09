# Kafka 3D Streaming Visualization (Streamlit)

ローカル Kafka（Docker Compose）で JSON ストリームを流し、Streamlit + Plotly で 3D 可視化します。
<img width="1411" height="816" alt="image" src="https://github.com/user-attachments/assets/29c9a489-2d10-41d0-a806-7d89c742b1e3" />


## セットアップ

```bash
# 1) 依存インストール（uv）
uv venv
uv pip install -e .

# 2) Kafka 起動（別ターミナルで）
docker compose up -d

# 3) プロデューサ起動（JSON を送信）
uv run python producer.py

# 4) Streamlit 可視化
uv run streamlit run app.py
```

- 環境変数
  - `KAFKA_BOOTSTRAP` (デフォルト: `localhost:9094`)
  - `KAFKA_TOPIC` (デフォルト: `stream3d`)

## 構成
- `docker-compose.yml`: Zookeeper + Kafka (Confluent 7.5 イメージ)
- `producer.py`: テスト用の JSON（3D 座標など）を Kafka に送信
- `consumer.py`: Kafka コンシューマ（Streamlit 内で利用）
- `app.py`: Streamlit UI（Plotly で 3D 点群をリアルタイム描画）

## 備考
- Kafka のトピックは自動作成を有効化。必要なら事前に作成してください。
- 表示点数やポーリング間隔はサイドバーから調整可能です。


