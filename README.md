# SimpleDHT
Although the design is based on Chord, it is a simplified version of Chord; node failures are not
handled. Therefore, there are three things this project implements:
1) ID space partitioning/re-partitioning,
2) Ring-based routing, and
3) Node joins.

The content provider implements all DHT functionality (including communication using sockets) and
supports insert and query operations. When running multiple instances of the app, all content
provider instances form a Chord ring and serve insert/query requests in a distributed
fashion according to the Chord protocol, meaning content providers only store <key , value> pairs to its corresponding partition.

Open [readme.pdf](https://github.com/Cabbler25/SimpleDHT/blob/master/readme.pdf) to view complete project specifications and instructions on how to test. Credits to Steve Ko from the University at Buffalo for python scripts and much of the content of the PDF. Run the following commands to create & test fixed AVDs in Android Studio.
```python
python create_avd.py 5
python run_avd.py 5
python set_redir.py 10000
```
