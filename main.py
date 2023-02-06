import xml.etree.ElementTree as et
import os 
# returnVersion="2018v3.1"
path = os.path.expanduser('~/Downloads/Form_990_Series_(e-file)_XML_2020/download990xml_2020_1/202000069349300255_public.xml')
mytree = et.parse(path)
myroot = mytree.getroot()
ns = {'irs':'http://www.irs.gov/efile'}
header = myroot.findall("./irs:ReturnHeader/irs:Filer/irs:EIN", ns)
print(header[0].text)

#  returnVersion="2017v2.2"
path2 = os.path.expanduser('~/Downloads/Form_990_Series_(e-file)_XML_2020/download990xml_2020_1/202000069349200200_public.xml')
mytree2 = et.parse(path2)
myroot2 = mytree2.getroot()
ein = myroot2.findall("./irs:ReturnHeader/irs:Filer/irs:EIN", ns)
print(ein[0].text)