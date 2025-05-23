import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
from collections import deque
import time

def consume_from_kafka_streamlit(topic: str = 'fact_table',
                                 bootstrap_servers: str = 'localhost:9092') -> None:
    """
    Consume messages from a Kafka topic and display them in real time using Streamlit.

    Args:
        topic (str): Kafka topic name.
        bootstrap_servers (str): Kafka server URL.
    """
    st.set_page_config(page_title="Leukemia Live WBC Monitor", layout="wide")
    
    with open("style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

    st.title("📊 Live WBC Count Dashboard")
    st.markdown("Streaming **Leukemia Analysis** data in real time from Kafka.")

    with st.sidebar:
        st.header("⚙️ Configuración")
        st.write(f"**Kafka Topic:** `{topic}`")
        st.write(f"**Kafka Server:** `{bootstrap_servers}`")
        st.info("Make sure the producer is sending data.")

    data = deque(maxlen=1000)
    placeholder = st.empty()

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='leukemia-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    except Exception as e:
        st.error(f"No se pudo conectar a Kafka: {e}")
        return

    for message in consumer:
        row = message.value
        data.append(row)

        df = pd.DataFrame(data)

        with placeholder.container():
            st.subheader("📈 Last 100 entries")
            col1, col2 = st.columns([3, 1])

            with col1:
                st.dataframe(df.tail(100), use_container_width=True)

            with col2:
                if 'wbc_count' in df.columns:
                    latest_wbc = df['wbc_count'].iloc[-1]
                    st.metric("Last WBC Count", f"{latest_wbc}")
                else:
                    st.warning("No se encontró la columna 'wbc_count' en los datos.")

            st.divider()

            if 'wbc_count' in df.columns:
                st.subheader("📊 WBC Tendency (last 50)")
                st.line_chart(df['wbc_count'].tail(50))

        time.sleep(0.5)


if __name__ == "__main__":
    consume_from_kafka_streamlit(topic='fact_table')
