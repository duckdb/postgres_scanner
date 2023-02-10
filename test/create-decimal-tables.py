import random

pattern = "123456789123456789123456789"

max_len = 18
for type_inte_len in range(0, max_len + 1):
	for type_frac_len in range(0, max_len + 1):
		type_len = type_inte_len + type_frac_len
		if type_len == 0:
			continue
		tname = f"dec_{type_inte_len}_{type_frac_len}"
		print(f"CREATE TABLE {tname} (dec DECIMAL({type_len},{type_frac_len}));")

		print(f"INSERT INTO {tname} VALUES ")
		for val_inte_len in range(0, type_inte_len + 1):
			for val_frac_len in range(0, type_frac_len + 1):
				inte_str = "0"
				frac_str = "0"
				if val_inte_len > 0:
					inte_str = pattern[0:val_inte_len]
				if val_frac_len > 0:
					frac_str = pattern[0:val_frac_len]
				print (f"({inte_str}.{frac_str}),")
				print (f"(-{inte_str}.{frac_str}),")
				if val_inte_len > 0 and val_frac_len > 0:
					print ("("+"1".ljust(val_inte_len, '0') + "." + "1".rjust(val_frac_len, '0') + "),")
					print ("(-"+"1".ljust(val_inte_len, '0') + "." + "1".rjust(val_frac_len, '0') + "),")
		print("(0.0);")
