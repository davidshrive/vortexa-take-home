import os
import pandas as pd
import time
import csv

# Classes
class Port:
	def __init__(self, name, diesel_abs = 0, diesel_rel = 1, crude_abs = 0, crude_rel = 1):
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

	def load_cargo_crude(self, cargo_amount):
		self.crude_abs = self.crude_abs - cargo_amount

	def discharge_cargo(self, cargo_type, cargo_amount):
		if cargo_type == "diesel":
			self.discharge_cargo_diesel(cargo_amount)
		elif cargo_type == "crude oil":
			self.discharge_cargo_crude(cargo_amount)

	def discharge_cargo_diesel(self, cargo_amount):
		self.diesel_abs = self.diesel_abs + cargo_amount

	def discharge_cargo_crude(self, cargo_amount):
		self.crude_abs = self.crude_abs + cargo_amount

	def csv_headers(self):
		return ["port","diesel","crude"]

	def csv_export(self):
		return {"port": self.name, "diesel": self.diesel_abs, "crude": self.crude_abs}

# Importers
def import_local_parquet(filename):
	return pd.read_parquet(filename)

def import_local_parquet_to_ports(filename):
	storage_df = import_local_parquet("./data/storage_asof_20200101.parquet")

	ports = {}
	for index, storage in storage_df.iterrows():
		port = Port(storage["port"], storage["diesel_absolute"], storage["diesel_relative"], storage["crude_absolute"], storage["crude_relative"])
		ports[storage["port"]] = port

	return ports


# Processors
def process_cargo_against_ports(cargos, ports, cargo_filter = False, timestamp = False):

	## Sort cargo data by timestamp
	cargos = cargos.sort_values(by="start_timestamp")

	## If timestamp arg provided create target time
	if timestamp:
		target_time = pd.to_datetime(timestamp)
	else:
		target_time = False

	## Procces all cargo and update each port if load/discharge event happens before target time
	for index, cargo in cargos.iterrows():

		load = cargo["loading_port"]
		load_time = pd.to_datetime(cargo["start_timestamp"])
		discharge = cargo["discharge_port"]
		discharge_time = pd.to_datetime(cargo["end_timestamp"])
		product = cargo["product"]
		quantity = cargo["quantity"]

		if (not cargo_filter) or (product in cargo_filter):
			# Load cargo from port
			if ((not target_time) or load_time < target_time) and (load in ports):
				ports[load].load_cargo(product, quantity)
			# Discharge cargo to port
			if ((not target_time) or discharge_time < target_time) and (discharge in ports):
				ports[discharge].discharge_cargo(product, quantity)

	return ports

# Exportors
def export_local_csv(ports, filename):
	with open(filename, "w") as outputFile:
		# Write headers
		writer = csv.DictWriter(outputFile, ports[list(ports.keys())[0]].csv_headers())
		writer.writeheader()

		# Write data
		for port in ports.values():
			writer.writerow(port.csv_export())



## Load storage and cargo data
ports = import_local_parquet_to_ports("./data/storage_asof_20200101.parquet")
cargo_df = import_local_parquet("./data/cargo_movements.parquet")

## Process cargo data against ports using cargo and time filters
ports = process_cargo_against_ports(cargo_df, ports, ['crude oil', 'diesel'], "2020-01-14 00:00:00")

## Export to csv
export_local_csv(ports, "output/ports.csv")