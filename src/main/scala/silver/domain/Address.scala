package silver.domain

case class Address(
    address_id: BigInt,
    address_country_code_iso2: String,
    address_city: String,
    address_street: String,
    address_postal_code: String
)
