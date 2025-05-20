import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
from collections import deque
import altair as alt

def consume_from_kafka_streamlit(topic: str = 'fact_table',
                                 bootstrap_servers: str = 'localhost:9092') -> None:
    """
    Consume messages from a Kafka topic and display them in real time using Streamlit.

    Args:
        topic (str): Name of the Kafka topic to consume from.
        bootstrap_servers (str): Kafka server address.
    """
    st.set_page_config(page_title="Monitoreo WBC", layout="wide")
    
    with st.container():
        st.title("📊 Real-Time Stream: WBC Count")
        st.markdown("### 🧬 Monitoreo en vivo del conteo de glóbulos blancos (WBC)")
        st.markdown("Visualización en tiempo real de datos relacionados con leucemia desde Kafka")

    placeholder = st.empty()
    data = deque(maxlen=1000)

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='leukemia-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        st.success("🟢 Conectado al broker Kafka")
    except Exception as e:
        st.error(f"❌ No se pudo conectar al broker Kafka: {e}")
        return

    run_stream = st.toggle("▶️ Iniciar visualización en tiempo real", value=True)

    if run_stream:
        for message in consumer:
            row = message.value  # Ejemplo: {"wbc_count": 6.2}
            data.append(row)
            df = pd.DataFrame(data)

            with placeholder.container():
                if not df.empty and 'wbc_count' in df.columns:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric(label="🧪 Último WBC", value=round(df['wbc_count'].iloc[-1], 2))
                    with col2:
                        st.metric(label="📈 Máximo", value=round(df['wbc_count'].max(), 2))
                    with col3:
                        st.metric(label="📉 Mínimo", value=round(df['wbc_count'].min(), 2))

                    st.dataframe(df.tail(100), use_container_width=True)

                    chart = alt.Chart(df.tail(50).reset_index()).mark_line(point=True).encode(
                        x=alt.X('index:Q', title='Últimos registros'),
                        y=alt.Y('wbc_count:Q', title='Conteo WBC'),
                        tooltip=['index', 'wbc_count']
                    ).properties(
                        width='container',
                        height=300,
                        title='Tendencia reciente del conteo WBC'
                    ).configure_axis(
                        labelFontSize=12,
                        titleFontSize=14
                    ).configure_title(
                        fontSize=16
                    )
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info("Esperando datos de Kafka...")

