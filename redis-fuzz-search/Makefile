all:
	cargo build
	$(MAKE) -C src

link:
	rm fuzz
	$(MAKE) -C src

.PHONY: clean
clean:
	cargo clean
	$(MAKE) -C src clean
