from faker import Faker
import pandas as pd

fake = Faker()
data = [{
    'product_id': i,
    'product_name': fake.word().capitalize() + " Product",
    'category': fake.random_element(elements=('Electronics', 'Books', 'Clothing', 'Home')),
    'price': round(fake.random_number(digits=5) / 100.0, 2)
} for i in range(1, 1001)]

df = pd.DataFrame(data)
df.to_csv('products.csv', index=False)
print("Generated products.csv")
