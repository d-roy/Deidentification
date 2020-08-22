name			 "research_DataImaging"
maintainer       "NaviNet Release Team"
maintainer_email "RELENG@navinet.net"
license          "Apache 2.0"
description      "http://git/git/?p=research_DataImaging.git"
long_description IO.read(File.join(File.dirname(__FILE__), 'README.md'))
version          "0.610.1"

# Contains Library for querying Artifactory for Maven artifacts
depends          "NaviNetProductInstaller", ">= 1.0.9"
depends "maven", ">= 3.1.1"