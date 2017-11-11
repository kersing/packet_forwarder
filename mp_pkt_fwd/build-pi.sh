#! /bin/bash
#
# Build script for Raspberry Pi based on resin build script
#

INSTALL_DIR="/opt/ttn-gateway"

mkdir -p $INSTALL_DIR/dev
cd $INSTALL_DIR/dev

if [ ! -d wiringPi ]; then
    git clone git://git.drogon.net/wiringPi  || { echo 'Cloning wiringPi failed.' ; exit 1; }
    cd wiringPi
else
    cd wiringPi
    git reset --hard
    git pull
fi
./build
cd ..

# alwas get the latest
rm -rf lora_gateway
git clone https://github.com/kersing/lora_gateway.git  || { echo 'Cloning lora_gateway failed.' ; exit 1; }

if [ ! -d paho.mqtt.embedded-c ]; then
    git clone https://github.com/kersing/paho.mqtt.embedded-c.git  || { echo 'Cloning paho mqtt failed.' ; exit 1; }
else
    cd paho.mqtt.embedded-c
    git reset --hard
    git pull
    cd ..
fi

if [ ! -d ttn-gateway-connector ]; then
    git clone https://github.com/kersing/ttn-gateway-connector.git  || { echo 'Cloning gateway connector failed.' ; exit 1; }
else
    cd ttn-gateway-connector
    git reset --hard
    git pull
    cd ..
fi

if [ ! -d protobuf-c ]; then
    git clone https://github.com/kersing/protobuf-c.git  || { echo 'Cloning protobuf-c failed.' ; exit 1; }
else
    cd protobuf-c
    git reset --hard
    git pull
    cd ..
fi

# make sure we get the latest
rm -rf packet_forwarder
git clone https://github.com/kersing/packet_forwarder.git  || { echo 'Cloning packet forwarder failed.' ; exit 1; }

if [ ! -d protobuf ]; then
    git clone https://github.com/google/protobuf.git  || { echo 'Cloning protobuf failed.' ; exit 1; }
else
    cd protobuf
    git reset --hard
    git pull
    cd ..
fi

apt-get update
apt-get -y install protobuf-compiler libprotobuf-dev libprotoc-dev automake libtool autoconf python-dev python-rpi.gpio

cd $INSTALL_DIR/dev/lora_gateway/libloragw
sed -i -e 's/PLATFORM= .*$/PLATFORM= imst_rpi/g' library.cfg
sed -i -e 's/CFG_SPI= .*$/CFG_SPI= native/g' library.cfg
#sed -i -e 's/DEBUG_GPS= .*$/DEBUG_GPS= 1/g' library.cfg
make

cd $INSTALL_DIR/dev/protobuf-c
./autogen.sh
./configure
make protobuf-c/libprotobuf-c.la
mkdir bin
./libtool install /usr/bin/install -c protobuf-c/libprotobuf-c.la `pwd`/bin
rm -f `pwd`/bin/*so*

cd $INSTALL_DIR/dev/paho.mqtt.embedded-c/
make
make install

cd $INSTALL_DIR/dev/ttn-gateway-connector
cp config.mk.in config.mk
make
cp bin/libttn-gateway-connector.so /usr/lib/

cd $INSTALL_DIR/dev/packet_forwarder/mp_pkt_fwd/
make

# Copy things needed at runtime to where they'll be expected
cp $INSTALL_DIR/dev/packet_forwarder/mp_pkt_fwd/mp_pkt_fwd $INSTALL_DIR/mp_pkt_fwd

echo "Build & Installation Of Binaries Completed."
echo "Do not forget to get master configuration from"
echo "https://github.com/TheThingsNetwork/gateway-conf"
echo "and to create a local configuration as well"
