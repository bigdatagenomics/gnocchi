from tempfile import mkstemp
from shutil import move
from os import remove, close
import csv


""" Takes vcf file and removes any sample that has 0 in the position or chromosome field."""
def fixVCFFile(filename): 
	fh, output_file = mkstemp()
	with open(filename) as to_read:
		with open(output_file, "wb") as tmp_file:
			reader = csv.reader(to_read, delimiter = "\t")
			writer = csv.writer(tmp_file, delimiter = " ")
			isTrue = False
			isfirst = True
			for row in reader:     # read one row at a time
				if len(row) >= 2:
					if not row[0].startswith('#'):#'dbGaP SubjID':
						if row[0] != 0 or row[1] != 0:
						    writer.writerow(row) # write it
					else: 
						writer.writerow(row)
	close(fh)
	move(output_file,"fixed_"+filename)

name = input("VCF filename:")
fixVCFFile(name)
