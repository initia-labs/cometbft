# Bloom params test

Dataset: `initiation-2`  5000000 ~ 7000000

Query: `propose_output.bridge_id=822`

Section size: 4096

- Bloom bits: 2048, Bloom values: 3 (SIze: 2.5G)
    
    `total 1995968 match 1037 falsePositive 60059
    search rate: 3.06%
    false positive rate: 98.30%
    consumed time: 186.67 seconds`
    
- 4096 / 3 (3.0G)
    
    `total 1995968 match 1037 falsePositive 6870
    search rate: 0.40%
    false positive rate: 86.89%
    consumed time: 27.10 seconds` 
    
- 8192 / 3 (3.6G)
    
    `total 1995968 match 1037 falsePositive 1188
    search rate: 0.11%
    false positive rate: 53.39%
    consumed time: 8.33 seconds`
    
- 16384 / 3 (4.4G)
    
    `total 1995968 match 1037 falsePositive 231
    search rate: 0.06%
    false positive rate: 18.22%
    consumed time: 4.77 seconds`
    
- **32768 / 3 (5.4G)**
    
    `total 1995968 match 1037 falsePositive 39
    search rate: 0.05%
    false positive rate: 3.62%
    consumed time: 2.55 seconds`
    
- 65536 / 3 (6.7G)
    
    `total 1995968 match 1037 falsePositive 7
    search rate: 0.05%
    false positive rate: 0.67%
    consumed time: 3.50 seconds`
    
- 2048 / 4 (2.6G)
    
    `total 1995968 match 1037 falsePositive 49920
    search rate: 2.55%
    false positive rate: 97.96%
    consumed time: 134.05 seconds`
    
- 4096 / 4 (3.1G)
    
    `total 1995968 match 1037 falsePositive 5761
    search rate: 0.34%
    false positive rate: 84.75%
    consumed time: 26.69 seconds`
    
- 8192 / 4 (3.8G)
    
    `total 1995968 match 1037 falsePositive 820
    search rate: 0.09%
    false positive rate: 44.16%
    consumed time: 7.82 seconds`
    
- 16384 / 4 (4.8G)
`total 1995968 match 1037 falsePositive 149
search rate: 0.06%
false positive rate: 12.56%
consumed time: 4.76 seconds`
- 16384 / 5 (5.1G)
    
    `total 1995968 match 1037 falsePositive 163
    search rate: 0.06%
    false positive rate: 13.58%
    consumed time: 4.99 seconds`
    
- 16384 / 6 (5.4G)
    
    `total 1995968 match 1037 falsePositive 167
    search rate: 0.06%
    false positive rate: 13.87%
    consumed time: 7.24 seconds`