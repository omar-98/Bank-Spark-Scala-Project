package silver.domain

import java.sql.Timestamp

case class Contact(
    contact_id: BigInt,
    contact_status: String,
    contact_first_name: String,
    contact_last_name: String,
    contact_email: String,
    contact_phone: String,
    contact_address_id: BigInt,
    join_date: Timestamp,
    date_of_birth: Timestamp
)
