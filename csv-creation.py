import pandas as pd
from faker import Faker
import random

# Initialize Faker
fake = Faker()

# Generate realistic logistics data
def generate_logistics_data(num_records):
    data = []
    for _ in range(num_records):
        shipment_id = fake.uuid4()
        order_id = fake.uuid4()
        customer_name = fake.name()
        customer_address = fake.address()
        customer_phone = fake.phone_number()
        shipping_date = fake.date_this_year()
        delivery_date = fake.date_between(start_date=shipping_date, end_date="+30d")
        shipment_status = random.choice(['In Transit', 'Delivered', 'Delayed', 'Pending'])
        carrier_name = random.choice(['DHL', 'FedEx', 'UPS', 'USPS'])
        tracking_number = fake.uuid4()
        shipping_method = random.choice(['Standard', 'Express', 'Overnight'])
        weight = round(random.uniform(0.5, 50.0), 2)  # Weight in kg
        dimensions = f"{round(random.uniform(5.0, 100.0), 2)}x{round(random.uniform(5.0, 100.0), 2)}x{round(random.uniform(5.0, 100.0), 2)}"  # LxWxH in cm
        origin_address = fake.address()
        destination_address = fake.address()
        shipment_cost = round(random.uniform(5.0, 500.0), 2)
        insurance = random.choice(['None', 'Basic', 'Premium'])
        special_instructions = fake.sentence(nb_words=6)
        remarks = fake.sentence(nb_words=10)
        data.append([
            shipment_id, order_id, customer_name, customer_address, customer_phone,
            shipping_date, delivery_date, shipment_status, carrier_name, tracking_number,
            shipping_method, weight, dimensions, origin_address, destination_address,
            shipment_cost, insurance, special_instructions, remarks
        ])
    return data

# Define column names
columns = [
    'shipmentId', 'orderId', 'customerName', 'customerAddress', 'customerPhone',
    'shippingDate', 'deliveryDate', 'shipmentStatus', 'carrierName', 'trackingNumber',
    'shippingMethod', 'weight', 'dimensions', 'originAddress', 'destinationAddress',
    'shipmentCost', 'insurance', 'specialInstructions', 'remarks'
]


num_records = 400
logistics_data = generate_logistics_data(num_records)

# Create a DataFrame and save to CSV
df = pd.DataFrame(logistics_data, columns=columns)
df.to_csv('logistics_data.csv', index=False)

print(f'{num_records} records written to logistics_data.csv')