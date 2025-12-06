import streamlit as st
import pandas as pd
import json
from kafka import KafkaProducer
from io import BytesIO
from PIL import Image
import os
import boto3
from datetime import datetime, timedelta

s3_client = boto3.client(
    's3',
    region_name='us-east-1',  
    aws_access_key_id= os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key= os.getenv('AWS_SECRET_ACCESS_KEY')
)

FALLBACK_IMAGE = "s3://notify-products/images/na_image/image_not_available.jpg"


current_user = {
    "user_id": "U0023",
    "name": "Elizabeth Sullivan",
    "email": "jerry25@porter.biz"
}
first_name = current_user["name"].split()[0]


def load_image_from_s3(s3_uri):
    try:
        bucket, key = s3_uri.replace("s3://", "").split("/", 1)
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        img = Image.open(BytesIO(obj['Body'].read()))
        return img
    except:
        print(f"Failed to load {s3_uri}, using fallback image.")
        # Load fallback image
        bucket, key = FALLBACK_IMAGE.replace("s3://", "").split("/", 1)
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        img = Image.open(BytesIO(obj['Body'].read()))
        return img

@st.cache_data(show_spinner=False)
def get_s3_image(s3_uri):
    return load_image_from_s3(s3_uri)

def check_user_alerts(user_id):
    """Check DynamoDB for user notifications"""
    try:
        print("Checking alerts for user:", user_id)
        # alerts_table = dynamodb.Table('user_notifications')
        # response = alerts_table.get_item(Key={'user_id': user_id})
        
        # # Get the notification string
        # notification = response.get('Item', {}).get('notification', '')
        
        # return notification if notification else ''
        return "Stay warm in style! Explore our latest winter collection with fresh arrivals and use code HURRAY10 to get 10% off" 
    except Exception as e:
        return ''


def create_view_dialog(row, img):
    @st.dialog(f"View: {row['productDisplayName']}")
    def show_dialog():
        st.image(img, caption=row["productDisplayName"], width=200)
        st.write(f"**Category:** {row['masterCategory']} | **Color:** {row['baseColour']}")
        st.write(f"**Article Type:** {row['articleType']}")
        st.title(f"Rs. {float(row.get('price', 0)):.2f}")
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

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('notify_products')

# Scan table (for small datasets)
response = table.scan()
items = response['Items']

# Convert to DataFrame
df = pd.DataFrame(items)

# -----------------------------
# Session State Initialization
# -----------------------------
if "cart" not in st.session_state:
    st.session_state.cart = {}  # Changed to dict to track quantities

if "click_log" not in st.session_state:
    st.session_state.click_log = []

if "user_notification" not in st.session_state:
    st.session_state.user_notification = ''

# Initialize notifications checking
if "last_alert_check" not in st.session_state:
    st.session_state.last_alert_check = datetime.now()
    print(st.session_state.last_alert_check)

# Track first page load (only skip toast on initial load)
if "initial_load" not in st.session_state:
    st.session_state.initial_load = True

# Track whether the user has dismissed / seen notifications
if "seen_notifications" not in st.session_state:
    st.session_state.seen_notifications = set()

# Use fragment with auto-refresh to check notifications without losing session
@st.fragment(run_every=40)  # Run every 300 seconds (5 minutes)
def check_notifications_background():
    """Background task to check for new notifications"""
    new_notification = check_user_alerts(current_user["user_id"])
    now = datetime.now()
    time_diff = (now - st.session_state.last_alert_check).total_seconds()
    print(f"Time since last alert check: {time_diff} seconds")

    # Only show toast if > 30 seconds since last toast
    if new_notification and time_diff >= 40:
        print("sending notification...")
        st.session_state.user_notification = new_notification
        st.toast(
                f"🔔 {new_notification}",
                icon="📬",
                duration=10000)
        st.session_state.seen_notifications.add(new_notification)
        st.session_state.last_alert_check = now

# Run the background check
check_notifications_background()


# -----------------------------
# Sidebar Cart
# -----------------------------
with st.sidebar:
    st.markdown(f"## 👤 Welcome, {first_name}!")
    st.markdown("---")
    st.title("🛒 Cart")
    
    if len(st.session_state.cart) == 0:
        st.info("Your cart is empty. Start shopping!")
    else:
        cart_total = 0
        for product_id, item in st.session_state.cart.items():
            item_total = float(item['price']) * item['quantity']
            cart_total += item_total
            with st.container():
                st.markdown(f"""
                <div style='padding: 0.75rem; border-radius: 0.5rem; margin: 0.5rem 0; border: 1px solid rgba(128, 128, 128, 0.2);'>
                    <strong>{item['name']}</strong><br>
                    <small>Quantity: {item['quantity']} × Rs. {item['price']}</small><br>
                    <small><strong>Subtotal: Rs. {item_total:.2f}</strong></small>
                </div>
                """, unsafe_allow_html=True)
    
    st.markdown("---")
    total_items = sum(item['quantity'] for item in st.session_state.cart.values())
    st.markdown(f"### 📦 Total items: **{total_items}**")
    
    if len(st.session_state.cart) > 0:
        st.markdown(f"### 💰 Total: **Rs. {cart_total:.2f}**")




st.markdown("""
    <style>
    /* Remove background gradient - let Streamlit handle it */
    
    h1 {
        text-align: left !important;
        margin-bottom: 2rem !important;
    }
    .block-container {
        max-width: 95% !important;
        padding-left: 2rem !important;
        padding-right: 2rem !important;
    }
    div[data-testid="column"] {
        display: flex !important;
        flex-direction: column !important;
    }
    div[data-testid="stVerticalBlock"] {
        gap: 0.5rem !important;
    }
    /* Center images in container */
    div[data-testid="stImage"] {
        display: flex !important;
        justify-content: center !important;
        align-items: center !important;
        padding: 1rem 0 !important;
    }
    /* Add hover effect to product cards - target the container div */
    div[data-testid="column"] > div > div > div[data-testid="stVerticalBlock"] {
        transition: transform 0.3s ease, box-shadow 0.3s ease !important;
        cursor: pointer !important;
        display: flex !important;
        flex-direction: column !important;
        height: 100% !important;
    }
    div[data-testid="column"] > div > div > div[data-testid="stVerticalBlock"]:hover {
        transform: translateY(-8px) !important;
        box-shadow: 0 12px 24px rgba(128, 128, 128, 0.3) !important;
    }
    /* Align quantity control buttons */
    div[data-testid="column"] button {
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        height: 2.5rem !important;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        padding-top: 2rem !important;
    }
    [data-testid="stSidebar"] h1 {
        font-size: 1.8rem !important;
        margin-bottom: 1.5rem !important;
    }
    /* Cart item styling */
    [data-testid="stSidebar"] ul {
        list-style: none !important;
        padding: 0 !important;
    }
    [data-testid="stSidebar"] li {
        padding: 0.75rem !important;
        margin: 0.5rem 0 !important;
        border-radius: 0.5rem !important;
        border: 1px solid rgba(128, 128, 128, 0.2) !important;
        transition: all 0.2s ease !important;
    }
    [data-testid="stSidebar"] li:hover {
        transform: translateX(5px) !important;
        box-shadow: 0 2px 8px rgba(128, 128, 128, 0.2) !important;
    }
    </style>
""", unsafe_allow_html=True)

st.title("🛍 Product Catalog")

# Notification bell icon in the top right
col1, col2 = st.columns([6, 1])
with col2:
    if st.session_state.user_notification:
        if st.button("🔔 (1)", key="notification_bell"):
            st.session_state.show_notification = not st.session_state.get('show_notification', False)

# Add some spacing after the title
st.markdown("<br>", unsafe_allow_html=True)

# Display notification panel if toggled
if st.session_state.get('show_notification', False) and st.session_state.user_notification:
    with st.expander("📬 Notification", expanded=True):
        st.info(st.session_state.user_notification)
        if st.button("Clear Notification"):
            st.session_state.user_notification = ''
            st.session_state.show_notification = False
            st.rerun()


# Add some spacing after the title
st.markdown("<br>", unsafe_allow_html=True)

# Increase number of columns from 3 to 5
cols = st.columns(6, gap="medium")


for idx, row in df.head(60).iterrows():
    col = cols[idx % 6]  # Changed from 3 to 5
    product_id = int(row["product_id"])
    img_url = row["image_url"]

    with col:
        with st.container(border=True, height= 490):
            img = get_s3_image(row["image_url"])
            st.image(img, width= 120)
            st.write(f"**{row['productDisplayName']}**")
            st.caption(f"{row['baseColour']} | {row['articleType']}")
            # Add spacer to push buttons to bottom
            st.markdown("<div style='flex-grow: 1;'></div>", unsafe_allow_html=True)

            # Price display
            price = float(row.get('price', 0))
            st.markdown(f"<h3 style='margin: 0; padding: 0.25rem 0;'>Rs. {price:.2f}</h3>", unsafe_allow_html=True)
             # Add spacer to push buttons to bottom
            st.markdown("<br>", unsafe_allow_html=True)
            view_dialog = create_view_dialog(row, img)

            # -------------------------------
            # VIEW button triggers modal / lightbox
            # -------------------------------
            if st.button("👁 View", key=f"view_{product_id}", use_container_width=True):
                # Log view in session click_log (optional backend use)
                st.session_state.click_log.append({
                    "action": "view",
                    "product_id": product_id,
                    "product_name": row["productDisplayName"],
                    "price": price
                }) 

                # Send Kafka view event
                producer.send("clickstream", {
                    "event": "view",
                    "user_id": current_user["user_id"],
                    "price": price,
                    "product_id": product_id,
                    "product_name": row["productDisplayName"],
                    "category": row["masterCategory"],
                    "article_type": row["articleType"],
                    "timestamp": pd.Timestamp.now().isoformat()
                })

                view_dialog()

            # -------------------------------
            # ADD TO CART button or Quantity controls
            # -------------------------------
            if product_id in st.session_state.cart:
                # Show quantity controls
                cols_qty = st.columns([1, 2, 1])
                with cols_qty[0]:
                    if st.button("➖", key=f"minus_{product_id}", use_container_width=True):
                        if st.session_state.cart[product_id]['quantity'] > 1:
                            st.session_state.cart[product_id]['quantity'] -= 1
                        else:
                            del st.session_state.cart[product_id]
                        st.rerun()
                
                with cols_qty[1]:
                    st.markdown(f"<div style='text-align: center; padding: 0.5rem; border-radius: 0.5rem; font-weight: bold;'>{st.session_state.cart[product_id]['quantity']} in cart</div>", unsafe_allow_html=True)
                
                with cols_qty[2]:
                    if st.button("➕", key=f"plus_{product_id}", use_container_width=True):
                        st.session_state.cart[product_id]['quantity'] += 1
                        st.rerun()
            else:
                # Show Add to Cart button
                if st.button("➕ Add to Cart", key=f"add_{product_id}", use_container_width=True):
                    st.session_state.cart[product_id] = {
                        'name': row['productDisplayName'],
                        'quantity': 1,
                        'details': row.to_dict(),
                        'price': price
                    }

                    # Send add_to_cart event to Kafka
                    producer.send("clickstream", {
                        "event": "add_to_cart",
                        "user_id": current_user["user_id"],
                        "price": price,
                        "product_id": product_id,
                        "product_name": row["productDisplayName"],
                        "article_type": row["articleType"],
                        "category": row["masterCategory"],
                        "timestamp": pd.Timestamp.now().isoformat()
                    })

                    st.rerun()