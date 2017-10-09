# Schemas

## Genotypic Dataset
Schema:
```
root
 |-- uniqueID: string (nullable = true)
 |-- chromosome: integer (nullable = true)
 |-- position: integer (nullable = true)
 |-- referenceAllele: string (nullable = true)
 |-- alternateAllele: string (nullable = true)
 |-- qualityScore: integer (nullable = true)
 |-- filter: string (nullable = true)
 |-- info: string (nullable = true)
 |-- format: string (nullable = true)
 |-- samples: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- sampleID: string (nullable = true)
 |    |    |-- value: string (nullable = true)
```

Row Level Case Class:
```
case class CalledVariant(uniqueID: String,
                         chromosome: Int,
                         position: Int,
                         referenceAllele: String,
                         alternateAllele: String,
                         qualityScore: Int,
                         filter: String,
                         info: String,
                         format: String,
                         samples: List[GenotypeState])
```

Example:
```
+---------+----------+--------+---------------+---------------+------------+------+----+------+--------------------+
| uniqueID|chromosome|position|referenceAllele|alternateAllele|qualityScore|filter|info|format|             samples|
+---------+----------+--------+---------------+---------------+------------+------+----+------+--------------------+
|rs3131974|         1|  752724|              C|              T|          60|  PASS|   .|    GT|[[sample1,0/1], [...|
|rs3131975|         1|  752725|              G|              A|          60|  PASS|   .|    GT|[[sample1,0/0], [...|
|rs3131973|         1|  752723|              C|              G|          60|  PASS|   .|    GT|[[sample1,0/1], [...|
|rs3131972|         1|  752722|              A|              T|          60|  PASS|   .|    GT|[[sample1,1/1], [...|
|rs3131971|         1|  752721|              A|              G|          60|  PASS|   .|    GT|[[sample1,0/0], [...|
+---------+----------+--------+---------------+---------------+------------+------+----+------+--------------------+
```

#### Overview 
This schema comes directly from the VCF file format. Currently have chosen to not support
the key-value tags that are included at the top of the VCF. **This is subject to change**. 
Based off of what features are to be supported in the future there may be 
reasons to include the key-value tags in the genotypic dataset. 

Our genotypic dataset that will be used throughout gnocchi is then represented as a 
`Dataset[CalledVariant]` object

#### Rationale
It is important to store the variant level information somewhere in the 
dataset, but it is common to an entire row. We can save on space if we 
keep the row structure of data and do not copy the common variant metadata
into each `GenotypeState` object (as it was previously).

## Phenotypic Dataset
#### Overview 
The phenotype information is stored as a scala `Map` object of 