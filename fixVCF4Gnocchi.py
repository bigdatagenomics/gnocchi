from tempfile import mkstemp
from shutil import move
from os import remove, close
import csv


""" Takes vcf file and sets all chromosome positions to 5"""
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
						myRow = [5, 5] + list(row[i] for i in range(2,len(row)))  
						writer.writerow(myRow) # write it
					else: 
						writer.writerow(row)
	close(fh)
	move(output_file,"fixed_"+filename)

name = input("VCF filename:")
fixVCFFile(name)
