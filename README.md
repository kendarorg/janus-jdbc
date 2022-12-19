## Janus Jdbc-Json-Jdbc Transparent Proxy Driver-On Development

Actually with 44% coverage

The project is born as a future extension of [HttpAnsweringMachine](https://github.com/kendarorg/HttpAnsweringMachine) (on github too)

The idea is to create a proxy Jdbc driver able to translate all JDBC calls into something serializable,
therefore able to be passed through simple http calls, and easy to modify.

The Server (Actually a simple serializer-deserializer) create the real connections/statements/resultsets
and exposes them via String data

So using the driver means

* Passing a "real" connection string to the db like "jdbc:h2:mem:test"
* Connect to the Janus-server like "jdbc:janus:http://localhost/db?fetchSize=3&charset=UTF-8"
* Look all messages passing

Tested (poorly but tested) with Hibernate

## Implementation

### Server

Actually only a "fake" implementation exists on tests: JsonServer

* private JdbcCommand getIjCommand(JdbcCommand command) translate the commands to serializable
* private JdbcResult getIjResult(JdbcResult command) translate the results to serializable

Simple index based reproduction of dataset interactions

### Serialization

A custom serializer is built to guarantee the type integrity. You can implement wetheaver you want: JsonTypedSerializer

An example of what is serialized

<pre>
{
	"command": {
		"database": "jdbc:janus:http://localhost/db?fetchSize=3&charset=UTF-8",
		":database:": "java.lang.String",
		"clientinfo": [{
			"_key": "janus.client.address",
			":_key:": "java.lang.String",
			"_value": "192.168.1.20",
			":_value:": "java.lang.String"
		}, {
			"_key": "janus.client.name",
			":_key:": "java.lang.String",
			"_value": "XPS15-KENDAR",
			":_value:": "java.lang.String"
		}],
		":clientinfo:": "java.util.Properties"
	},
	":command:": "org.kendar.janus.cmd.connection.ConnectionConnect"
}
</pre>

Here a command remotely invoked via reflection

<pre>
{
	"command": {
		"name": "cancel",
		":name:": "java.lang.String",
		"paramtype": [],
		":paramtype:": "[Ljava.lang.Class;",
		"[paramtype]": "0",
		"parameters": [],
		":parameters:": "[Ljava.lang.Object;",
		"[parameters]": "0"
	},
	":command:": "org.kendar.janus.cmd.Exec"
}
</pre>

The things are

* "name":"value"  parameter content
* ":name:":"JAVA_CLASS" parameter type
* "[name]":"1,10" array dimensions
* "_key":"value" key of hashmap
* "_value":"value" value of hashmap
* "_":"value" value of array

## Dirty tricks

* The Updatable ResultSets are fetched one by one
* RowId is handled as RowId when present, converted to long/String/byte[] if founded matching
* Conversions extending "[Java Type Conversion (Done Well)](https://github.com/toddfast/typeconverter)"
