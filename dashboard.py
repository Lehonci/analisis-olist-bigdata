import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from config import DB_CONN_PARAMS, ANALYTICS_TABLES

# ─────────────────────────────────────────────────────────
# Konfigurasi Halaman Streamlit
# ─────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Olist E-Commerce Big Data & Regional Clustering Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .main-title {
        font-size: 2.1rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 2px;
    }
    .sub-title {
        font-size: 0.98rem;
        color: #475569;
        margin-bottom: 18px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────────────────────
# Pembaca Tabel Hasil Olahan Apache Spark (Analytics Mart)
# Tanpa Query Pengolahan SQL Biasa & Tanpa Data Hardcode
# ─────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_analytics_table(table_key: str) -> pd.DataFrame:
    """Membaca tabel hasil olahan Apache Spark dari layer Analytics Mart di PostgreSQL."""
    table_name = ANALYTICS_TABLES[table_key]
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONN_PARAMS)
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name}")
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(rows, columns=columns)
    except Exception:
        return pd.DataFrame()
    finally:
        if conn is not None:
            conn.close()


# ─────────────────────────────────────────────────────────
# Header & Sidebar
# ─────────────────────────────────────────────────────────
st.markdown(
    '<p class="main-title">🛒 Segmentasi Wilayah Pelanggan E-Commerce Olist (K-Means & Big Data)</p>',
    unsafe_allow_html=True,
)
st.markdown(
    '<p class="sub-title">Pipeline End-to-End: 9 CSV Olist ➔ Apache Kafka ➔ PostgreSQL Staging ➔ Apache Spark (Spark SQL + MLlib K-Means) ➔ Analytics Mart ➔ Streamlit</p>',
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("⚙️ Arsitektur & Kontrol")
    if st.button("🔄 Muat Ulang Data Spark Mart", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.info(
        """
        **Skema Data Pipeline:**
        - **Ingestion**: Apache Kafka (9 Topik)
        - **Raw Storage**: PostgreSQL (`staging_*`)
        - **Processing & ML**: Apache Spark (`eda_spark.py`)
        - **Serving Layer**: Spark Analytics Mart (`analytics_*`)
        - **Metode Klasterisasi**: PySpark MLlib K-Means (Z-Score + Elbow + Silhouette)
        """
    )
    st.markdown("---")
    st.markdown("**Tim Peneliti (TRPL SV IPB):**")
    st.caption("• Josua Abrianto Harianja (J0403241062)")
    st.caption("• Zafira Ryani Putri (J0403241139)")
    st.caption("• Ben Azaria (J0403241135)")
    st.caption("• Nur'Afia Avanza (J0403241008)")

# Muat seluruh tabel hasil olahan Spark
df_kpi = load_analytics_table("kpi_summary")
df_pay = load_analytics_table("payment_dist")
df_status = load_analytics_table("order_status")
df_monthly = load_analytics_table("monthly_trend")
df_cities = load_analytics_table("top_cities")
df_categories = load_analytics_table("top_categories")
df_eval = load_analytics_table("kmeans_eval")
df_states = load_analytics_table("state_clusters")
df_cluster_sum = load_analytics_table("cluster_summary")
df_cluster_prod = load_analytics_table("cluster_products")
df_density = load_analytics_table("density_comparison")

if df_kpi.empty or df_states.empty or df_eval.empty:
    st.error(
        "⚠️ Tabel hasil analitik Apache Spark (`analytics_*`) belum ditemukan di PostgreSQL. "
        "Pastikan Anda telah menjalankan perintah `python eda_spark.py` terlebih dahulu."
    )
    st.stop()

kpi_row = df_kpi.iloc[0]

tab1, tab2, tab3, tab4 = st.tabs(
    [
        "📊 Ringkasan & KPI",
        "🗺️ Sebaran Geografis (RQ1)",
        "📦 Kategori Produk & Preferensi Klaster (RQ3)",
        "🤖 K-Means Clustering & Pola Konsumsi (RQ2 & RQ4)",
    ]
)

# ─────────────────────────────────────────────────────────
# TAB 1: RINGKASAN & KPI EKOSISTEM E-COMMERCE
# ─────────────────────────────────────────────────────────
with tab1:
    st.subheader("Ringkasan Metrik Utama Hasil Olahan Spark SQL")

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Pelanggan Unik", f"{int(kpi_row['total_unique_customers']):,}")
    c2.metric("Pesanan Terkirim", f"{int(kpi_row['total_delivered_orders']):,}")
    c3.metric("Total Pendapatan", f"R$ {float(kpi_row['total_revenue']):,.2f}")
    c4.metric("Rata-rata Pesanan", f"R$ {float(kpi_row['avg_order_value']):,.2f}")
    c5.metric("Rata-rata Ongkir", f"R$ {float(kpi_row['avg_freight_value']):,.2f}")
    c6.metric("Skor Ulasan", f"⭐ {float(kpi_row['avg_review_score']):.2f} / 5.0")

    st.markdown("---")

    if not df_monthly.empty:
        st.markdown("##### 📅 Tren Volume Pesanan dan Nilai Pendapatan Bulanan (2017–2018)")
        fig_trend = go.Figure()
        fig_trend.add_trace(
            go.Bar(
                x=df_monthly["order_month"],
                y=df_monthly["total_orders"],
                name="Total Pesanan Terkirim",
                marker_color="#3B82F6",
                yaxis="y1",
            )
        )
        fig_trend.add_trace(
            go.Scatter(
                x=df_monthly["order_month"],
                y=df_monthly["total_revenue"],
                name="Total Pendapatan (R$)",
                mode="lines+markers",
                line=dict(color="#DC2626", width=3),
                yaxis="y2",
            )
        )
        fig_trend.update_layout(
            xaxis=dict(title="Bulan Transaksi"),
            yaxis=dict(title="Jumlah Pesanan", side="left"),
            yaxis2=dict(title="Total Pendapatan (R$)", side="right", overlaying="y", showgrid=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=390,
            margin=dict(l=20, r=20, t=40, b=20),
        )
        st.plotly_chart(fig_trend, use_container_width=True)

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown("##### 💳 Distribusi Metode Pembayaran")
        if not df_pay.empty:
            fig_pay = px.pie(
                df_pay,
                values="total_value",
                names="payment_type",
                hover_data=["total_transactions", "avg_value", "pct_value"],
                title="Proporsi Nilai Transaksi per Metode Pembayaran (R$)",
                hole=0.42,
                color_discrete_sequence=px.colors.qualitative.Safe,
            )
            st.plotly_chart(fig_pay, use_container_width=True)

    with col_r:
        st.markdown("##### 📈 Distribusi Status Pesanan")
        if not df_status.empty:
            fig_status = px.bar(
                df_status.sort_values("total_orders", ascending=True),
                x="total_orders",
                y="order_status",
                orientation="h",
                text="pct_orders",
                title="Jumlah & Persentase Pesanan Berdasarkan Status",
                labels={"total_orders": "Jumlah Pesanan", "order_status": "Status Pesanan"},
                color="total_orders",
                color_continuous_scale="Blues",
            )
            fig_status.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
            st.plotly_chart(fig_status, use_container_width=True)

# ─────────────────────────────────────────────────────────
# TAB 2: SEBARAN GEOGRAFIS PELANGGAN & TRANSAKSI (RQ1)
# ─────────────────────────────────────────────────────────
with tab2:
    st.subheader("🗺️ [RQ1] Wilayah dengan Jumlah Pelanggan dan Volume Transaksi Tertinggi")

    col_geo_map, col_geo_bar = st.columns([1.15, 1])

    with col_geo_map:
        st.markdown("##### 🌎 Peta Interaktif Sebaran Pelanggan & Klaster Wilayah di Brasil")
        fig_map = px.scatter_geo(
            df_states,
            lat="lat",
            lon="lng",
            size="total_customers",
            color="cluster_name",
            hover_name="customer_state",
            hover_data={
                "total_customers": ":,.0f",
                "total_orders": ":,.0f",
                "total_revenue": ":,.2f",
                "avg_order_value": ":,.2f",
                "avg_freight_value": ":,.2f",
                "avg_review_score": ":.2f",
                "lat": False,
                "lng": False,
            },
            size_max=45,
            scope="south america",
            title="Peta Distribusi Pelanggan Unik & Klaster Negara Bagian Brasil",
            color_discrete_sequence=["#1D4ED8", "#10B981", "#F59E0B"],
        )
        fig_map.update_geos(
            center=dict(lat=-14.235, lon=-51.925),
            projection_scale=2.4,
            showcountries=True,
            countrycolor="#CBD5E1",
            showland=True,
            landcolor="#F8FAFC",
        )
        fig_map.update_layout(height=470, margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig_map, use_container_width=True)

    with col_geo_bar:
        st.markdown("##### 🏆 Top 10 Negara Bagian Berdasarkan Jumlah Pelanggan Unik")
        top10_states = df_states.sort_values("total_customers", ascending=False).head(10)
        fig_state_cust = px.bar(
            top10_states.sort_values("total_customers", ascending=True),
            x="total_customers",
            y="customer_state",
            orientation="h",
            color="total_revenue",
            color_continuous_scale="Viridis",
            text="total_customers",
            labels={
                "customer_state": "Negara Bagian (State)",
                "total_customers": "Pelanggan Unik",
                "total_revenue": "Pendapatan (R$)",
            },
            title="10 Negara Bagian dengan Pelanggan Unik & Pendapatan Tertinggi",
        )
        fig_state_cust.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
        fig_state_cust.update_layout(height=470)
        st.plotly_chart(fig_state_cust, use_container_width=True)

    col_city, col_freight = st.columns(2)

    with col_city:
        st.markdown("##### 🏙️ Top 10 Kota dengan Jumlah Pelanggan Unik Tertinggi")
        if not df_cities.empty:
            top10_cities = df_cities.sort_values("total_unique_customers", ascending=False).head(10).copy()
            top10_cities["city_label"] = (
                top10_cities["customer_city"].str.title() + " (" + top10_cities["customer_state"] + ")"
            )
            fig_cities = px.bar(
                top10_cities.sort_values("total_unique_customers", ascending=True),
                x="total_unique_customers",
                y="city_label",
                orientation="h",
                color="total_revenue",
                color_continuous_scale="Tealgrn",
                labels={
                    "city_label": "Kota (State)",
                    "total_unique_customers": "Pelanggan Unik",
                    "total_revenue": "Total Pendapatan (R$)",
                },
                title="10 Kota Terbesar Berdasarkan Pelanggan Unik",
            )
            st.plotly_chart(fig_cities, use_container_width=True)

    with col_freight:
        st.markdown("##### 🚚 10 Negara Bagian dengan Rata-Rata Ongkos Kirim Tertinggi")
        top10_freight = df_states.sort_values("avg_freight_value", ascending=False).head(10)
        fig_freight = px.bar(
            top10_freight.sort_values("avg_freight_value", ascending=True),
            x="avg_freight_value",
            y="customer_state",
            orientation="h",
            color="avg_order_value",
            color_continuous_scale="OrRd",
            text="avg_freight_value",
            labels={
                "customer_state": "Negara Bagian",
                "avg_freight_value": "Rata-rata Ongkir (R$)",
                "avg_order_value": "Rata-rata Nilai Pesanan (R$)",
            },
            title="10 Negara Bagian dengan Beban Ongkos Kirim Tertinggi",
        )
        fig_freight.update_traces(texttemplate="R$ %{text:.2f}", textposition="outside")
        st.plotly_chart(fig_freight, use_container_width=True)

    st.markdown("##### 📋 Tabel Lengkap 7 Fitur + Indikator Logistik 27 Negara Bagian Brasil")
    st.dataframe(
        df_states[
            [
                "customer_state",
                "cluster",
                "cluster_name",
                "total_customers",
                "total_orders",
                "total_revenue",
                "avg_order_value",
                "total_items",
                "avg_freight_value",
                "freight_to_price_pct",
                "avg_delivery_days",
                "avg_review_score",
            ]
        ].sort_values("total_revenue", ascending=False),
        use_container_width=True,
        hide_index=True,
    )

# ─────────────────────────────────────────────────────────
# TAB 3: ANALISIS KATEGORI PRODUK NASIONAL & PER KLASTER (RQ3)
# ─────────────────────────────────────────────────────────
with tab3:
    st.subheader("📦 [RQ3] Kategori Produk Terpopuler Nasional & Dominasi per Klaster Wilayah")

    if not df_categories.empty:
        top15_cat = df_categories.sort_values("total_items_sold", ascending=False).head(15)
        col_p1, col_p2 = st.columns(2)

        with col_p1:
            fig_cat_vol = px.bar(
                top15_cat.sort_values("total_items_sold", ascending=True),
                x="total_items_sold",
                y="category_name",
                orientation="h",
                color="total_items_sold",
                color_continuous_scale="Teal",
                labels={"total_items_sold": "Jumlah Item Terjual", "category_name": "Kategori Produk"},
                title="Top 15 Kategori Produk Berdasarkan Volume Unit Terjual",
            )
            st.plotly_chart(fig_cat_vol, use_container_width=True)

        with col_p2:
            top15_sales = df_categories.sort_values("total_sales", ascending=False).head(15)
            fig_cat_sales = px.bar(
                top15_sales.sort_values("total_sales", ascending=True),
                x="total_sales",
                y="category_name",
                orientation="h",
                color="total_sales",
                color_continuous_scale="Plasma",
                labels={"total_sales": "Total Nilai Penjualan (R$)", "category_name": "Kategori Produk"},
                title="Top 15 Kategori Produk Berdasarkan Nilai Penjualan (R$)",
            )
            st.plotly_chart(fig_cat_sales, use_container_width=True)

    st.markdown("---")
    st.markdown("#### 🎯 [RQ3] Perbandingan Kategori Produk Dominan pada Tiap Klaster Wilayah")

    if not df_cluster_prod.empty:
        fig_cluster_cat = px.bar(
            df_cluster_prod,
            x="category_name",
            y="pct_of_cluster_items",
            color="cluster_name",
            barmode="group",
            text="rank",
            hover_data=["total_sold", "total_revenue", "avg_price", "avg_freight"],
            labels={
                "category_name": "Kategori Produk",
                "pct_of_cluster_items": "Porsi terhadap Total Item Klaster (%)",
                "cluster_name": "Klaster Wilayah",
            },
            title="Porsi 5 Kategori Produk Teratas terhadap Total Pembelian di Masing-Masing Klaster (%)",
        )
        fig_cluster_cat.update_traces(texttemplate="#%{text}", textposition="outside")
        st.plotly_chart(fig_cluster_cat, use_container_width=True)

        clusters_sorted = sorted(df_cluster_prod["cluster"].unique())
        cols_cp = st.columns(len(clusters_sorted))
        for idx, cid in enumerate(clusters_sorted):
            sub_df = df_cluster_prod[df_cluster_prod["cluster"] == cid].sort_values("rank")
            c_name = sub_df["cluster_name"].iloc[0]
            with cols_cp[idx]:
                st.markdown(f"**Klaster {cid}: {c_name}**")
                st.dataframe(
                    sub_df[
                        [
                            "rank",
                            "category_name",
                            "total_sold",
                            "pct_of_cluster_items",
                            "total_revenue",
                            "avg_price",
                        ]
                    ].rename(
                        columns={
                            "rank": "#",
                            "category_name": "Kategori",
                            "total_sold": "Unit",
                            "pct_of_cluster_items": "Porsi (%)",
                            "total_revenue": "Penjualan (R$)",
                            "avg_price": "Harga Rata2 (R$)",
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

# ─────────────────────────────────────────────────────────
# TAB 4: K-MEANS CLUSTERING & ANALISIS KEPADATAN (RQ2 & RQ4)
# ─────────────────────────────────────────────────────────
with tab4:
    st.subheader("🤖 [RQ2] Evaluasi Jumlah Klaster Optimal (Elbow Method & Silhouette Score)")

    df_eval_sorted = df_eval.sort_values("k")
    opt_rows = df_eval_sorted[df_eval_sorted["is_optimal"] == True]
    opt_row = opt_rows.iloc[0] if not opt_rows.empty else df_eval_sorted.iloc[1]
    opt_k = int(opt_row["k"])

    col_e1, col_e2 = st.columns(2)

    with col_e1:
        fig_elbow = go.Figure()
        fig_elbow.add_trace(
            go.Scatter(
                x=df_eval_sorted["k"],
                y=df_eval_sorted["wssse_cost"],
                mode="lines+markers",
                name="WSSSE (Inertia)",
                line=dict(color="#2563EB", width=3),
                marker=dict(size=10),
            )
        )
        fig_elbow.add_trace(
            go.Scatter(
                x=[opt_k],
                y=[float(opt_row["wssse_cost"])],
                mode="markers+text",
                name=f"Titik Siku Optimal (K={opt_k})",
                text=[f"Optimal Elbow K={opt_k}"],
                textposition="top right",
                marker=dict(size=16, color="#DC2626", symbol="star"),
            )
        )
        fig_elbow.update_layout(
            title=f"Kurva Elbow Method (WSSSE) — Penurunan Terbesar pada K={opt_k} (-{float(opt_row['wssse_drop_pct']):.1f}%)",
            xaxis_title="Jumlah Klaster (K)",
            yaxis_title="Within-Cluster Sum of Squared Errors (WSSSE)",
            height=380,
        )
        st.plotly_chart(fig_elbow, use_container_width=True)

    with col_e2:
        fig_sil = px.bar(
            df_eval_sorted,
            x="k",
            y="silhouette_score",
            color="elbow_distance",
            text="silhouette_score",
            color_continuous_scale="Viridis",
            labels={
                "k": "Jumlah Klaster (K)",
                "silhouette_score": "Silhouette Score",
                "elbow_distance": "Jarak Siku (Knee Distance)",
            },
            title=f"Evaluasi Silhouette Score & Knee Distance (K={opt_k} Terpilih)",
        )
        fig_sil.update_traces(texttemplate="%{text:.4f}", textposition="outside")
        fig_sil.update_layout(height=380)
        st.plotly_chart(fig_sil, use_container_width=True)

    st.dataframe(
        df_eval_sorted[
            [
                "k",
                "wssse_cost",
                "wssse_drop_pct",
                "elbow_distance",
                "silhouette_score",
                "non_singleton_clusters",
                "is_optimal",
            ]
        ].rename(
            columns={
                "k": "K",
                "wssse_cost": "WSSSE (Inertia)",
                "wssse_drop_pct": "Penurunan WSSSE (%)",
                "elbow_distance": "Jarak Titik Siku (Kneedle)",
                "silhouette_score": "Silhouette Score",
                "non_singleton_clusters": "Jumlah Klaster Multi-Negara Bagian",
                "is_optimal": "Klaster Optimal",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("---")
    st.subheader(f"📊 Profil Karakteristik {opt_k} Klaster Negara Bagian Brasil (RQ2)")

    col_sc1, col_sc2 = st.columns(2)
    with col_sc1:
        fig_sc_vol = px.scatter(
            df_states,
            x="total_orders",
            y="total_revenue",
            size="total_customers",
            color="cluster_name",
            hover_name="customer_state",
            hover_data=["avg_order_value", "avg_freight_value", "avg_review_score"],
            log_x=True,
            log_y=True,
            title="Peta Klaster: Volume Pesanan vs Total Pendapatan (Skala Logaritmik)",
            labels={
                "total_orders": "Total Pesanan (Log Scale)",
                "total_revenue": "Total Pendapatan R$ (Log Scale)",
                "cluster_name": "Klaster Wilayah",
            },
            color_discrete_sequence=["#1D4ED8", "#10B981", "#F59E0B"],
        )
        st.plotly_chart(fig_sc_vol, use_container_width=True)

    with col_sc2:
        fig_sc_log = px.scatter(
            df_states,
            x="avg_freight_value",
            y="avg_order_value",
            size="total_customers",
            color="cluster_name",
            text="customer_state",
            hover_name="customer_state",
            hover_data=["total_customers", "freight_to_price_pct", "avg_delivery_days", "avg_review_score"],
            title="Hubungan Rata-Rata Ongkos Kirim vs Nilai Pesanan Rata-Rata (RQ4)",
            labels={
                "avg_freight_value": "Rata-rata Ongkos Kirim per Pesanan (R$)",
                "avg_order_value": "Nilai Rata-rata Pesanan / Ticket Size (R$)",
                "cluster_name": "Klaster Wilayah",
            },
            color_discrete_sequence=["#1D4ED8", "#10B981", "#F59E0B"],
        )
        fig_sc_log.update_traces(textposition="top center")
        st.plotly_chart(fig_sc_log, use_container_width=True)

    if not df_cluster_sum.empty:
        st.markdown("##### 📋 Ringkasan Statistik Centroid & Anggota Klaster (`analytics_cluster_summary`)")
        st.dataframe(
            df_cluster_sum[
                [
                    "cluster",
                    "cluster_name",
                    "total_states",
                    "states_list",
                    "avg_customers_per_state",
                    "avg_orders_per_state",
                    "avg_revenue_per_state",
                    "avg_order_ticket_size",
                    "avg_freight",
                    "avg_freight_ratio_pct",
                    "avg_delivery_days",
                    "avg_satisfaction",
                ]
            ].rename(
                columns={
                    "cluster": "ID",
                    "cluster_name": "Nama Klaster",
                    "total_states": "Jml State",
                    "states_list": "Daftar Negara Bagian",
                    "avg_customers_per_state": "Rata2 Pelanggan",
                    "avg_orders_per_state": "Rata2 Pesanan",
                    "avg_revenue_per_state": "Rata2 Pendapatan (R$)",
                    "avg_order_ticket_size": "Nilai Pesanan Rata2 (R$)",
                    "avg_freight": "Rata2 Ongkir (R$)",
                    "avg_freight_ratio_pct": "Rasio Ongkir/Harga (%)",
                    "avg_delivery_days": "Durasi Kirim (Hari)",
                    "avg_satisfaction": "Skor Ulasan",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("##### 💡 Rekomendasi Strategi Pemasaran & Distribusi Berbasis Data Spark")
        for _, row in df_cluster_sum.sort_values("cluster").iterrows():
            st.info(
                f"**Klaster {int(row['cluster'])} — {row['cluster_name']} ({int(row['total_states'])} Negara Bagian: {row['states_list']})**\n\n"
                f"• **Karakteristik Utama**: Rata-rata pelanggan unik **{float(row['avg_customers_per_state']):,.1f}**, "
                f"nilai pesanan rata-rata **R$ {float(row['avg_order_ticket_size']):,.2f}**, "
                f"rata-rata ongkos kirim **R$ {float(row['avg_freight']):,.2f}** "
                f"({float(row['avg_freight_ratio_pct']):.2f}% dari harga produk), "
                f"waktu pengiriman rata-rata **{float(row['avg_delivery_days']):.1f} hari**, "
                f"dan skor kepuasan **{float(row['avg_satisfaction']):.2f}/5.00**.\n"
                f"• **Rekomendasi Strategi**: {row['strategy_recommendation']}"
            )

    st.markdown("---")
    st.subheader("⚖️ [RQ4] Perbedaan Pola Konsumsi Wilayah Kepadatan Pelanggan Tinggi vs Rendah")

    if not df_density.empty:
        col_d1, col_d2 = st.columns(2)
        with col_d1:
            fig_rq4_a = px.bar(
                df_density,
                x="density_tier",
                y=["avg_order_value", "avg_freight_value"],
                barmode="group",
                text_auto=".2f",
                title="Perbandingan Nilai Pesanan Rata-Rata vs Ongkos Kirim (R$)",
                labels={"value": "Nilai dalam Real Brasil (R$)", "density_tier": "Kelompok Kepadatan Wilayah"},
                color_discrete_sequence=["#0EA5E9", "#F43F5E"],
            )
            st.plotly_chart(fig_rq4_a, use_container_width=True)

        with col_d2:
            fig_rq4_b = px.bar(
                df_density,
                x="density_tier",
                y=["share_customers_pct", "share_revenue_pct", "freight_to_price_pct"],
                barmode="group",
                text_auto=".2f",
                title="Pangsa Pelanggan (%), Pangsa Pendapatan (%), & Rasio Beban Ongkir (%)",
                labels={"value": "Persentase (%)", "density_tier": "Kelompok Kepadatan Wilayah"},
                color_discrete_sequence=["#6366F1", "#10B981", "#F59E0B"],
            )
            st.plotly_chart(fig_rq4_b, use_container_width=True)

        st.dataframe(
            df_density.rename(
                columns={
                    "cluster": "ID Klaster",
                    "density_tier": "Tingkat Kepadatan Wilayah",
                    "total_states": "Jumlah Negara Bagian",
                    "share_customers_pct": "Pangsa Pelanggan Nasional (%)",
                    "share_orders_pct": "Pangsa Pesanan Nasional (%)",
                    "share_revenue_pct": "Pangsa Pendapatan Nasional (%)",
                    "avg_customers_per_state": "Rata2 Pelanggan/State",
                    "avg_order_value": "Nilai Pesanan Rata2 (R$)",
                    "avg_items_per_order": "Rata2 Item/Pesanan",
                    "avg_freight_value": "Rata2 Ongkir (R$)",
                    "freight_to_price_pct": "Rasio Ongkir thd Harga (%)",
                    "avg_delivery_days": "Rata2 Waktu Kirim (Hari)",
                    "avg_review_score": "Rata2 Skor Ulasan",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
