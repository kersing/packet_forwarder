### Environment constants 

LGW_PATH ?= ../../lora_gateway/libloragw
ARCH ?=
CROSS_COMPILE ?=
export

### general build targets

all:
	$(MAKE) all -e -C mp_pkt_fwd

clean:
	$(MAKE) clean -e -C mp_pkt_fwd


### EOF
