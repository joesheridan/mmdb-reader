## Synopsis

MaxMind publish an IP geolocation database in their custom .mmdb format.
This repo contains code to parse the data in the database and return the geo-location information for a given IP address.

## How The MMDB Format Works

There are three distinct sections in the mmdb file format:
The Meta Data Section
The Binary Search Tree
The Geo Data Records

First, we find the metadata section which describes the number of nodes in the binary search tree and the size of the search tree records.
Then we take the binary representation of the IP address and for each bit, we take the left or right path through the binary search tree depending on whether the value was 0 or 1. Eventually we will land on a node which gives us a pointer to the geo-data record.
The geo data record is in a binary format comprised of fields which begin with a control byte which indicates which type of field it is. Common fields include Maps, Strings, Pointers and variable length format Ints.
Once we have parsed the geo data record, we can extract the country iso code which indicates which country the given IP is registered to.

## Code Example

import mmdb.MMDBReader
...
MMDBReader.lookupIP("path-to-GeoLite2-Country.mmdb", "some-IP-String")

The lookupIP function returns a Try[String]

## Installation

## Dependencies

The code is based on the following:
MaxMind GeoLite2-Country.mmdb database
Scala 2.11.8

## License