yum install -y epel-release
yum install -y python-pip
yum install -y python-devel
yum install -y gcc-c++
yum install -y librdkafka-devel
rpm -e --nodeps mapr-librdkafka
yum install -y mapr-librdkafka
pip install --upgrade pip
pip install --global-option=build_ext --global-option="--library-dirs=/opt/mapr/lib" --global-option="--include-dirs=/opt/mapr/include/" mapr-streams-python
export LD_LIBRARY_PATH=:/opt/mapr/lib:/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.181-3.b13.el7_5.x86_64/jre/lib/amd64/server
yum remove -y librdkafka

wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
yum install -y apache-maven
pip install maprdb


