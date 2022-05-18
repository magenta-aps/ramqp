<!--
SPDX-FileCopyrightText: 2021 Magenta ApS <https://magenta.dk>
SPDX-License-Identifier: MPL-2.0
-->

CHANGELOG
=========

2.0.0 - 2022-05-18
------------------

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

1.3.1 - 2022-05-05
------------------

[#49896] Hotfix bug introduced by restructure metrics and add publish metrics

1.3.0 - 2022-05-04
------------------

[#49896] Restructure metrics and add publish metrics

1.2.0 - 2022-05-04
------------------

[#49896] Added healthcheck endpoint

1.1.1 - 2022-04-25
------------------

[#49706] Fix MO AMQP routing key to correspond with MOs

1.1.0 - 2022-04-20
------------------

[#49610] MOAMQPSystem register non-unique wrappers

1.0.0 - 2022-04-12
------------------

[#49610] Initial release

0.1.2 - 2022-04-04
------------------

[#49610] Testing autopub

0.1.1 - 2022-04-03
------------------

Manual release

0.1.0 - 2022-04-03
------------------

Manual release
