## Janus Jdbc-Json-Jdbc Transparent Proxy Driver

The project is born as a future extension of [HttpAnsweringMachine](https://github.com/kendarorg/HttpAnsweringMachine) (on github too)

The idea is to create a proxy Jdbc driver able to translate all JDBC calls into something serializable,
therefore able to be passed through simple http calls, and easy to modify.

The Server (Actually a simple serializer-deserializer) create the real connections/statements/resultsets
and exposes them via String data

So using the driver means

