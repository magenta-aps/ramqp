Release type: major

[#49706] Add individual amqp_scheme, amqp_host, amqp_user, amqp_password, amqp_port, amqp_vhost fields.

Before, the AMQP server could only be configured using the `amqp_url` setting.
Now, AMQP connection settings can alternatively be set individually using 
`amqp_x` variables. Additionally, `queue_prefix` was renamed to
`amqp_queue_prefix` for consistency.
