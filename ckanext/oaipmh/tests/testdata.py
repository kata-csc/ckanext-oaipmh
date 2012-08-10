listrecords="""<?xml version='1.0' encoding='UTF-8'?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2012-08-10T10:41:21Z</responseDate>
  <request verb="ListRecords" metadataPrefix="oai_dc">/oai</request>
  <ListRecords>
    <record>
      <header>
        <identifier>eef495a8-0154-4e15-96a9-6c966928f46b</identifier>
        <datestamp>2012-08-10T10:20:12Z</datestamp>
        <setSpec>bunshin</setSpec>
      </header>
      <metadata>
        <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/" xsi:schemaLocation="http://purl.org/dc/elements/1.1/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
          <dc:title>bunshin</dc:title>
          <dc:creator>rdsega</dc:creator>
          <dc:subject>bar</dc:subject>
          <dc:subject>baz</dc:subject>
          <dc:subject>foo</dc:subject>
          <dc:description>aerdfgssrh</dc:description>
          <dc:date>2012-08-10</dc:date>
          <dc:type>dataset</dc:type>
          <dc:identifier>/dataset/eef495a8-0154-4e15-96a9-6c966928f46b</dc:identifier>
          <dc:identifier>eef495a8-0154-4e15-96a9-6c966928f46b</dc:identifier>
          <dc:rights>Open Data Commons Public Domain Dedication and Licence (PDDL)</dc:rights>
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
    <set>
      <setSpec>dddb6684-3cc3-4d23-9b1c-e089ebefb85a</setSpec>
      <setName>fee</setName>
    </set>
  </ListSets>
</OAI-PMH>

"""
listidentifiers="""<?xml version='1.0' encoding='UTF-8'?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
  <responseDate>2012-08-10T11:36:22Z</responseDate>
  <request verb="ListIdentifiers" metadataPrefix="oai_dc">/oai</request>
  <ListIdentifiers>
    <header>
      <identifier>eef495a8-0154-4e15-96a9-6c966928f46b</identifier>
      <datestamp>2012-08-10T10:20:12Z</datestamp>
      <setSpec>bunshin</setSpec>
    </header>
  </ListIdentifiers>
</OAI-PMH>

"""
