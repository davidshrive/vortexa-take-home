import os
import pandas as pd


class Port:
	def __init__(self,name , diesel_abs, diesel_rel, crude_abs, crude_rel):
		self.name = name
		self.diesel_abs = diesel_abs
		self.diesel_max = diesel_abs/diesel_rel
		self.crude_abs = crude_abs
		self.crude_max = crude_abs/crude_rel

	def __repr__(self):
		return f"Port Name: {self.name}\nDiesel: {self.diesel_abs}\nCrude: {self.crude_abs}"

## Load storage data
storage_file = "./data/storage_asof_20200101.parquet"
storage_df = pd.read_parquet(storage_file)
print(storage_df)

for index, storage in storage_df.iterrows():
	port = Port(storage['port'], storage['diesel_absolute'], storage['diesel_relative'], storage['crude_absolute'], storage['crude_relative'])
	print(port)
	exit()

## Load cargo data
cargo_file = "./data/cargo_movements.parquet"
cargo_df = pd.read_parquet(cargo_file)
#print(cargo_df)
