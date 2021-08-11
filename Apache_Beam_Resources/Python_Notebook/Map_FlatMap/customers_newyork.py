# -*- coding: utf-8 -*-
"""Lecture9_Customers_Newyork.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1yLEe40qi1xXedqVBcKYt92m6L4cPauFi
"""

import apache_beam as beam
p1 = beam.Pipeline()
customers = (
    p1
    |beam.io.ReadFromText('Customers_age.txt')
    |beam.Map(lambda record:record.split(','))
    |beam.Filter(lambda record: record[2]=='NY' and int(record[3])>20)
    |beam.io.WriteToText('result')
)
p1.run()

!cat result-00000-of-00001