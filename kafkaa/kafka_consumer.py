import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
import time


def consume_from_kafka_streamlit(topic: str = 'fact_table',
                                  bootstrap_servers: str = 'localhost:9092') -> None:
    """
    Consume messages from a Kafka topic and display them in real time using Streamlit.

    Args:
        topic (str): Name of the Kafka topic to consume from.
        bootstrap_servers (str): Kafka server address, typically 'localhost:9092' or Docker bridge IP.
    """
    st.title("📊 Real-Time Stream: WBC Count")
    st.markdown("Live visualization of leukemia data received from Kafka")

    placeholder = st.empty()
    data = []

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
        st.error(f"❌ Could not connect to Kafka broker: {e}")
        return

    for message in consumer:
        row = message.value  # e.g., {"wbc_count": 6.2}
        data.append(row)

        df = pd.DataFrame(data)

        placeholder.dataframe(df.tail(100), use_container_width=True)

        if len(data) > 1000:
            data = data[-500:]  # Retain only recent records

        time.sleep(0.5)


if __name__ == "__main__":
    consume_from_kafka_streamlit(topic='fact_table')
