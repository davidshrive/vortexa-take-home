import os
import pandas as pd


class Port:
	def __init__(self, name , diesel_abs, diesel_rel, crude_abs, crude_rel):
		self.name = name
		self.diesel_abs = diesel_abs
		self.diesel_max = diesel_abs/diesel_rel
		self.crude_abs = crude_abs
		self.crude_max = crude_abs/crude_rel

	def __repr__(self):
		return f"Port Name: {self.name}\nDiesel: {self.diesel_abs}\nCrude: {self.crude_abs}"

	def load_cargo(self, cargo_type, cargo_amount):
		if cargo_type == "diesel":
			self.load_cargo_diesel(cargo_amount)

	def load_cargo_diesel(self, cargo_amount):
		self.diesel_abs = self.diesel_abs - cargo_amount
		# Add check for max amount

	def discharge_cargo(self, cargo_type, cargo_amount):
		if cargo_type == "diesel":
			self.discharge_cargo_diesel(cargo_amount)

	def discharge_cargo_diesel(self, cargo_amount):
		self.diesel_abs = self.diesel_abs + cargo_amount
		# Add check for max amount


## Load storage data
storage_file = "./data/storage_asof_20200101.parquet"
storage_df = pd.read_parquet(storage_file)

ports = {}
for index, storage in storage_df.iterrows():
	port = Port(storage['port'], storage['diesel_absolute'], storage['diesel_relative'], storage['crude_absolute'], storage['crude_relative'])
	ports[storage['port']] = port

print(ports)

## Load cargo data
cargo_file = "./data/cargo_movements.parquet"
cargo_df = pd.read_parquet(cargo_file)
print(cargo_df)

for index, cargo in cargo_df.iterrows():
	print(cargo)

	load = cargo['loading_port']
	discharge = cargo['discharge_port']
	product = cargo['product']
	quantity = cargo['quantity']

	if product == 'diesel':
		# Load cargo from port
		ports[load].load_cargo(product, quantity)
		# Discharge cargo to port
		ports[discharge].discharge_cargo(product, quantity)

	print(ports)
	exit()
