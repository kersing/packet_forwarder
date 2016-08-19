### Environment constants 

LGW_PATH ?= ../../lora_gateway/libloragw
ARCH ?=
CROSS_COMPILE ?=
export

### general build targets

mac:
	$(MAKE) all -e -C poly_pkt_fwd
	$(MAKE) all -e -C util_ack
	$(MAKE) all -e -C util_sink
	$(MAKE) all -e -C util_tx_test

all:
	$(MAKE) all -e -C lora_pkt_fwd
	$(MAKE) all -e -C poly_pkt_fwd
	$(MAKE) all -e -C util_ack
	$(MAKE) all -e -C util_sink
	$(MAKE) all -e -C util_tx_test

clean:
	$(MAKE) clean -e -C poly_pkt_fwd
	$(MAKE) clean -e -C util_ack
	$(MAKE) clean -e -C util_sink
	$(MAKE) clean -e -C util_tx_test
	$(MAKE) clean -e -C lora_pkt_fwd


### EOF
