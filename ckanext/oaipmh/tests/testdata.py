# coding: utf-8
listrecords="""<?xml version='1.0' encoding='UTF-8'?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2012-08-10T10:41:21Z</responseDate>
  <request verb="ListRecords" metadataPrefix="oai_dc">/oai</request>
  <ListRecords>
    <record>
    <header>
        <identifier>oai:helda.helsinki.fi:1975/7634</identifier>
        <datestamp>2011-06-09T14:38:35Z</datestamp>
        <setSpec>hdl_10138_18081</setSpec>
    </header>
    <metadata>
        <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
            <dc:title>Perunan typpilannoitus luonnonmukaisessa viljelyssä</dc:title>
            <dc:creator>Tall, Anna</dc:creator>
            <dc:subject>peruna</dc:subject>
            <dc:subject>luonnonmukainen viljely</dc:subject>
            <dc:subject>sato</dc:subject>
            <dc:subject>määrä</dc:subject>
            <dc:subject>laatu</dc:subject>
            <dc:subject>viherlannoitus</dc:subject>
            <dc:contributor>Helsingin yliopisto, soveltavan biologian laitos</dc:contributor>
            <dc:date>2007</dc:date>
            <dc:type>Book</dc:type>
            <dc:identifier>http://hdl.handle.net/1975/7634</dc:identifier>
            <dc:identifier>1796-6361</dc:identifier>
            <dc:language>fi</dc:language>
            <dc:relation>Julkaisuja / Helsingin yliopisto, soveltavan biologian laitos</dc:relation>
            <dc:relation>34</dc:relation>
        </oai_dc:dc>
    </metadata>
</record>
  </ListRecords>
</OAI-PMH>

"""
listsets="""<?xml version='1.0' encoding='UTF-8'?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2012-08-10T11:29:29Z</responseDate>
  <request verb="ListSets">/oai</request>
  <ListSets>
    <set><setSpec>hdl_10138_14633</setSpec><setName>Abstracts</setName></set>
  </ListSets>
</OAI-PMH>

"""
listidentifiers="""<?xml version='1.0' encoding='UTF-8'?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2012-08-10T11:36:22Z</responseDate>
  <request verb="ListIdentifiers" metadataPrefix="oai_dc">/oai</request>
  <ListIdentifiers>
   <header>
   <identifier>oai:helda.helsinki.fi:1975/7634</identifier>
   <datestamp>2011-06-09T14:38:35Z</datestamp>
   <setSpec>hdl_10138_18081</setSpec>
   </header>
  </ListIdentifiers>
</OAI-PMH>

"""
nohierarchy="""<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" 
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2012-08-29T17:31:59Z</responseDate>
  <request verb="ListSets">http://jultika.oulu.fi/OAI/Server</request>
    <error code="noSetHierarchy">Sets not supported</error></OAI-PMH>
"""
identify="""<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" 
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2012-08-29T17:38:19Z</responseDate>
  <request verb="Identify">http://jultika.oulu.fi/OAI/Server</request>
    <Identify>
    <repositoryName>University of Oulu Repository - Jultika</repositoryName>
    <baseURL>http://jultika.oulu.fi/OAI/Server</baseURL>
    <protocolVersion>2.0</protocolVersion>
    <earliestDatestamp>2000-01-01T00:00:00Z</earliestDatestamp>
    <deletedRecord>transient</deletedRecord>
    <granularity>YYYY-MM-DDThh:mm:ssZ</granularity>
    <adminEmail>ville.varjonen@oulu.fi</adminEmail>
          <description>
        <oai-identifier xmlns="http://www.openarchives.org/OAI/2.0/oai-identifier"
                        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                        xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai-identifier
                                            http://www.openarchives.org/OAI/2.0/oai-identifier.xsd">
          <scheme>oai</scheme>
          <repositoryIdentifier>oulu.fi</repositoryIdentifier>
          <delimiter>:</delimiter>
          <sampleIdentifier>oai:oulu.fi:isbn978-951-42-9519-5</sampleIdentifier>
        </oai-identifier>
      </description>
      </Identify></OAI-PMH>
"""
