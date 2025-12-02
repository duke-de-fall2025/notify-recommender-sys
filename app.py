import streamlit as st
import pandas as pd
import json
from kafka import KafkaProducer
import os

def safe_image(path, caption= None, width=None):
    if caption:
        if os.path.exists(path):
            st.image(path, caption= caption, width=width)
        else:
            st.image("no_image_available.png", caption= caption, width=width) 
    else:
        if os.path.exists(path):
            st.image(path, width=width)
        else:
            st.image("no_image_available.png", width=width) 

def create_view_dialog(row, img_path):
    @st.dialog(f"View: {row['productDisplayName']}")
    def show_dialog():
        safe_image(img_path, caption=row["productDisplayName"], width=600)
        st.write(f"**Category:** {row['masterCategory']} | **Color:** {row['baseColour']}")
        st.button("Close")
    return show_dialog


# -----------------------------
# Kafka Producer
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# -----------------------------
# Load Product Data
# -----------------------------
styles_df = pd.read_csv("styles.csv", 
                        on_bad_lines='skip',     # skip problematic rows
                        engine='python'          # use Python engine for messy CSVs
    )

# -----------------------------
# Session State Initialization
# -----------------------------
if "cart" not in st.session_state:
    st.session_state.cart = []

if "last_added" not in st.session_state:
    st.session_state.last_added = None

if "click_log" not in st.session_state:
    st.session_state.click_log = []

# -----------------------------
# Sidebar Cart
# -----------------------------
with st.sidebar:
    st.title("🛒 Cart")
    if len(st.session_state.cart) == 0:
        st.write("Your cart is empty.")
    else:
        for item in st.session_state.cart:
            st.write(f"- {item['productDisplayName']}")
    st.write("---")
    st.write(f"**Total items:** {len(st.session_state.cart)}")

# -----------------------------
# Product Grid UI
# -----------------------------
st.title("🛍 Product Catalog")

cols = st.columns(3)


for idx, row in styles_df.head(12).iterrows():
    col = cols[idx % 3]
    product_id = int(row["id"])
    img_path = f"images/{product_id}.jpg"

    with col:

        safe_image(img_path, width=200)
        st.write(f"**{row['productDisplayName']}**")
        st.caption(f"{row['baseColour']} | {row['articleType']}")
        st.markdown("</div>", unsafe_allow_html=True)

        view_dialog = create_view_dialog(row, img_path)

        # -------------------------------
        # VIEW button triggers modal / lightbox
        # -------------------------------
        if st.button("👁 View", key=f"view_{product_id}"):
            # Log view in session click_log (optional backend use)
            st.session_state.click_log.append({
                "action": "view",
                "product_id": product_id,
                "product_name": row["productDisplayName"]
            })

            # Send Kafka view event
            producer.send("clickstream", {
                "event": "view",
                "product_id": product_id,
                "product_name": row["productDisplayName"],
                "category": row["masterCategory"],
                "timestamp": pd.Timestamp.now().isoformat()
            })

            view_dialog()

        # -------------------------------
        # ADD TO CART button
        # -------------------------------
        if st.button("➕ Add to Cart", key=f"add_{product_id}"):
            st.session_state.cart.append(row.to_dict())

            # Send add_to_cart event to Kafka
            producer.send("clickstream", {
                "event": "add_to_cart",
                "product_id": product_id,
                "product_name": row["productDisplayName"],
                "category": row["masterCategory"],
                "timestamp": pd.Timestamp.now().isoformat()
            })

            st.session_state.last_added = product_id
            st.rerun()

        # Show success message for last added item
        if st.session_state.last_added == product_id:
            st.success("Added to cart! 🎉")
            st.session_state.last_added = None
