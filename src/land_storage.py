import os
import pandas as pd
import time
import csv

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
		elif cargo_type == "crude oil":
			self.load_cargo_crude(cargo_amount)

	def load_cargo_diesel(self, cargo_amount):
		self.diesel_abs = self.diesel_abs - cargo_amount
		# Add check for max amount

	def load_cargo_crude(self, cargo_amount):
		self.crude_abs = self.crude_abs - cargo_amount
		# Add check for max amount

	def discharge_cargo(self, cargo_type, cargo_amount):
		if cargo_type == "diesel":
			self.discharge_cargo_diesel(cargo_amount)
		elif cargo_type == "crude oil":
			self.discharge_cargo_crude(cargo_amount)

	def discharge_cargo_diesel(self, cargo_amount):
		self.diesel_abs = self.diesel_abs + cargo_amount
		# Add check for max amount

	def discharge_cargo_crude(self, cargo_amount):
		self.crude_abs = self.crude_abs + cargo_amount
		# Add check for max amount

	def csv_headers(self):
		return ["port","diesel","crude"]

	def csv_export(self):
		return {"port": self.name, "diesel": self.diesel_abs, "crude": self.crude_abs}

## Load storage data
storage_file = "./data/storage_asof_20200101.parquet"
storage_df = pd.read_parquet(storage_file)

ports = {}
for index, storage in storage_df.iterrows():
	port = Port(storage["port"], storage["diesel_absolute"], storage["diesel_relative"], storage["crude_absolute"], storage["crude_relative"])
	ports[storage["port"]] = port

## Load cargo data
cargo_file = "./data/cargo_movements.parquet"
cargo_df = pd.read_parquet(cargo_file)

## Sort cargo data by timestamp
cargo_df = cargo_df.sort_values(by="start_timestamp")

target_time = pd.to_datetime("2020-01-14 00:00:00")
## Procces all cargo and update each port if load/discharge event happens before target timestamp
for index, cargo in cargo_df.iterrows():

	load = cargo["loading_port"]
	load_time = pd.to_datetime(cargo["start_timestamp"])
	discharge = cargo["discharge_port"]
	discharge_time = pd.to_datetime(cargo["end_timestamp"])
	product = cargo["product"]
	quantity = cargo["quantity"]

	if product in ["diesel","crude oil"]:
		# Load cargo from port
		if (load_time < target_time) and (load in ports):
			ports[load].load_cargo(product, quantity)
		# Discharge cargo to port
		if (discharge_time < target_time) and (discharge in ports):
			ports[discharge].discharge_cargo(product, quantity)

## Export
outputPath = "output/"
outputFilename = outputPath+"ports.csv"
with open(outputFilename, "w") as outputFile:
	# Write headers
	writer = csv.DictWriter(outputFile, ports["Houston"].csv_headers())
	writer.writeheader()

	# Write data
	for port in ports.values():
		writer.writerow(port.csv_export())