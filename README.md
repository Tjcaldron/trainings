// =====================
// BRONZE LAYER
// Raw ingestion tables
// =====================

Table HEADER_RAW {
  identifier varchar [pk]
  carrier_code varchar
  vessel_name varchar
  vessel_country_code varchar
  port_of_unlading varchar
  estimated_arrival_date date
  foreign_port_of_lading_qualifier varchar
  manifest_quantity int
  manifest_unit varchar
  weight decimal
  weight_unit varchar
  measurement decimal
  measurement_unit varchar
}

Table SHIPPER_RAW {
  identifier varchar [pk]
  shipper_party_name varchar
  shipper_party_address varchar
  shipper_party_address_2 varchar
  shipper_party_address_3 varchar
  shipper_party_address_4 varchar
  city varchar
  state_province varchar
  zip_code varchar
  country_code varchar
  country_name varchar
  comm_number_qualifier varchar
  comm_number varchar
}

Table CARGO_RAW {
  identifier varchar [pk]
  container_number varchar
  description_sequence_number int
  piece_count int
  description_text varchar
}

Table TARIFF_RAW {
  identifier varchar [pk]
  description_sequence_number int
  container_number varchar
  harmonized_number varchar
  harmonized_value decimal
  harmonized_weight_combined varchar
  harmonized_weight_kg decimal
}

Ref: HEADER_RAW.identifier < SHIPPER_RAW.identifier
Ref: HEADER_RAW.identifier < CARGO_RAW.identifier
Ref: HEADER_RAW.identifier < TARIFF_RAW.identifier

// =====================
// SILVER LAYER
// Cleaned, typed, normalized
// =====================

Table HEADER_SILVER {
  identifier varchar [pk]
  carrier_code varchar
  vessel_name varchar
  vessel_country_code varchar
  port_of_unlading varchar
  estimated_arrival_date date
  foreign_port_of_lading_qualifier varchar
  weight_combined decimal
  year int
  load_date date
  source_system varchar
}

Table SHIPPER_SILVER {
  identifier varchar [pk]
  shipper_party_name varchar
  city varchar
  state_province varchar
  zip_code varchar
  country_code varchar [ref: > COUNTRIES_SILVER.country_code]
  comm_number_qualifier varchar
  comm_number varchar
  year int
  load_date date
  source_system varchar
}

Table CARGO_SILVER {
  identifier varchar [pk]
  container_number varchar
  description_sequence_number int
  piece_count int
  description_text varchar
  year int
  load_date date
  source_system varchar
}

Table TARIFF_SILVER {
  identifier varchar [pk]
  description_sequence_number int
  container_number varchar
  harmonized_number varchar
  harmonized_value decimal
  harmonized_weight_combined varchar
  harmonized_weight_kg decimal
  year int
  load_date date
  source_system varchar
}

Table COUNTRIES_SILVER {
  country_code varchar [pk]
  country_name varchar
  year int
}

Ref: HEADER_SILVER.identifier < SHIPPER_SILVER.identifier
Ref: HEADER_SILVER.identifier < CARGO_SILVER.identifier
Ref: HEADER_SILVER.identifier < TARIFF_SILVER.identifier

// =====================
// GOLD LAYER
// Aggregated summary
// All three years combined
// =====================

Table TARIFF_AGG {
  identifier varchar [pk]
  year int [pk]
  total_harmonized_weight decimal
  harmonized_weight_unit varchar
  total_tariff_lines int
}

Table SHIPPER_COMBINED {
  identifier varchar [pk]
  shipper_party_name varchar
  country_name varchar
  year int
}

Table GOLD_SUMMARY {
  shipper_party_name varchar [pk]
  country_name varchar
  year int [pk]
  total_harmonized_weight decimal
  total_shipments int
  load_date date
  source_system varchar
}

Ref: TARIFF_AGG.identifier - GOLD_SUMMARY.shipper_party_name
Ref: SHIPPER_COMBINED.identifier - GOLD_SUMMARY.shipper_party_name
