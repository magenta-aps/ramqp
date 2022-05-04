Release type: major

[#49896] Modified the MO callback function parameters and loosed the MOAMQP interface a bit.


Before the MO Callback function had this signature:
```
Callable[[ServiceType, ObjectType, RequestType, PayloadType], Awaitable]
```
While now it has this signature:
```
Callback[[MORoutingKey, PayloadType], Awaitable]
```

Before MOAMQP's `register` and `publish_message` had very a strict interface,
while now the interface is looser using overloaded methods.
