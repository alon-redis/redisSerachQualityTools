include Makefile
print-%:
	@printf '%s\n' '$($*)'
