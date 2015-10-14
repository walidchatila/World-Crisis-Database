##!/usr/bin/env python

from xml.etree.ElementTree import fromstring, tostring, Element, TreeBuilder
import _mysql, io, MySQLdb, sys

_set = []
RES_List = []
WAY_List = []
CON_List = []
CIT_List = []
url_List = []

def WCDB_login () :
    """
    Log in database and set up a connection with Mysql.
    Hard coded the host, username, password and database. 
    """
    c = MySQLdb.Connection(
            host = "z",
            user = "walidc",
            passwd = "DylYysFx.Z",
            db = "cs327e_walidc")
    assert str(type(c)) == "<class 'MySQLdb.connections.Connection'>"
    return c

def WCDB_query (c, s) :
    """
    Make connections and takes a string that is then inserted into
    tables in Mysql. Will be used to populate the tables.
    """
    assert str(type(c)) == "<class 'MySQLdb.connections.Connection'>"
    assert(type(s) is str)
    c.query(s)
    r = c.use_result()
    if r is None :
        return None
    assert(str(type(r)) == "<type '_mysql.result'>")
    t = r.fetch_row(maxrows = 0)
    assert(type(t) is tuple)
    return t

def WCDB_create(c):
    """
    Populates the database with tables. Will make 20 tables that are 
    consistent with the schema. 
    """

    assert str(type(c)) == "<class 'MySQLdb.connections.Connection'>"

    t = WCDB_query(c,'drop table if exists Crises;')
    t = WCDB_query(c,'drop table if exists Orgs;')
    t = WCDB_query(c,'drop table if exists People;')
    t = WCDB_query(c,'drop table if exists Resources;')
    t = WCDB_query(c,'drop table if exists WaysToHelp;')
    t = WCDB_query(c,'drop table if exists ContactInfos;')
    t = WCDB_query(c,'drop table if exists Citations;')
    t = WCDB_query(c,'drop table if exists Urls;')
    t = WCDB_query(c,'drop table if exists CrisisResources;')
    t = WCDB_query(c,'drop table if exists CrisisWaysToHelp;')
    t = WCDB_query(c,'drop table if exists OrgContactInfos;')
    t = WCDB_query(c,'drop table if exists CrisisCitations;')
    t = WCDB_query(c,'drop table if exists OrgCitations;')
    t = WCDB_query(c,'drop table if exists PersonCitations;')
    t = WCDB_query(c,'drop table if exists CrisisUrls;')
    t = WCDB_query(c,'drop table if exists OrgUrls;')
    t = WCDB_query(c,'drop table if exists PersonUrls;')
    t = WCDB_query(c,'drop table if exists CrisisOrgs;')
    t = WCDB_query(c,'drop table if exists CrisisPeople;')
    t = WCDB_query(c,'drop table if exists OrgPeople;')
    

    t = WCDB_query(c,
        """
create table Crises (
crisisID varchar(7),
name text,
kind enum('Natural Disaster','War / Conflict','Act of Terrorism','Human Error Disaster','Assassination / Shooting'),
streetAddress text,
city text,
stateOrProvince text,
postalCode text,
country text,
dateAndTime text,
fatalities bigint,
injuries bigint,
populationIll bigint,
populationDisplaced bigint,
environmentalImpact text,
politicalChanges text,
culturalChanges text,
jobsLost bigint,
damageInUSD bigint,
reparationCost bigint,
regulatoryChanges text,
primary key (crisisID));
""")


    t = WCDB_query(c,
        """
create table Orgs (
orgID varchar(7),
name text,
kind enum('Government Agency','Military Force','Intergovernmental Agency','Intergovernmental Public Healthy Agency','Nonprofit / Humanitarian Organization'),
streetAddress text,
city text,
stateOrProvince text,
postalCode text,
country text,
foundingMission text,
dateFounded text,
dateAbolished text,
majorEvents text,
primary key (orgID));
""")

    t = WCDB_query(c,
        """
create table People (
personID varchar(7),
name text,
kind enum('Celebrity','Actor / Actress','Musician','Politician','President','CEO','Humanitarian','Perpetrator'),
streetAddress text,
city text,
stateOrProvince text,
postalCode text,
country text,
primary key (personID));
""")

    t = WCDB_query(c,
        """
create table Resources (
resourceID varchar(7),
resource text,
primary key (resourceID));
""")

    t = WCDB_query(c,
        """
create table WaysToHelp (
helpID varchar(7),
wayToHelp text,
primary key (helpID));
""")

    t = WCDB_query(c,
        """
create table ContactInfos (
contactInfoID varchar(7),
phoneNumber text,
emailAddress text,
facebookUrlID text,
twitterUrlID text,
websiteUrlID text,
primary key (contactInfoID));
""")

    t = WCDB_query(c,
        """
create table Citations (
citationID varchar(7),
citation text,
primary key (citationID));
""")

    t = WCDB_query(c,
        """
create table Urls (
urlID varchar(7),
type enum('Image','Video','Map','SocialNetwork','Website','ExternalLink'),
urlAddress text,
primary key(urlID));
""")

    t = WCDB_query(c,
        """
create table CrisisResources(
crisisID varchar(7),
resourceID varchar(7));
""")

    t = WCDB_query(c,
        """
create table CrisisWaysToHelp(
crisisID varchar(7),
helpID varchar(7));
""")

    t = WCDB_query(c,
        """
create table OrgContactInfos(
orgID varchar(7),
contactInfoID varchar(7));
""")

    t = WCDB_query(c,
        """
create table CrisisCitations(
citationID varchar(7),
crisisID varchar(7));
""")

    t = WCDB_query(c,
        """
create table OrgCitations(
orgID varchar(7),
citationID varchar(7));
""")

    t = WCDB_query(c,
        """
create table PersonCitations(
personID varchar(7),
citationID varchar(7));
""")

    t = WCDB_query(c,
        """
create table CrisisUrls(
crisisID varchar(7),
urlID varchar(7));
""")

    t = WCDB_query(c,
        """
create table OrgUrls(
orgID varchar(7),
urlID varchar(7));
""")

    t = WCDB_query(c,
        """
create table PersonUrls(
personID varchar(7),
urlID varchar(7));
""")

    t = WCDB_query(c,
        """
create table CrisisOrgs(
crisisID varchar(7),
orgID varchar(7));
""")

    t = WCDB_query(c,
        """
create table CrisisPeople(
crisisID varchar(7),
personID varchar(7));
""")

    t = WCDB_query(c,
        """
create table OrgPeople(
orgID varchar(7),
personID varchar(7));
""")


def WCDB_import(r, c):
    """
    Import will read in an XML and parse it.
    This will then be used to create queries.
    The queries will be used to populate the tables in the database. 
    This will also merge different databases together 
    """

    # Take in xml and turn into an element tree for parsing
    sql_query = ''
    s = ''.join(r)
    tree = fromstring(s)
    assert (type(tree) is Element)

    I = iter(tree)
    v = 0
    w = 0
    x = 0
    y = 0
    z = 0
    for a in range(1):
        for i in range(len(list(tree))):
            table = next(I)
            
            #take each row of the table, and then iterate through each column
            #Fills in the crises table
            if(table.tag == 'crises'):
                public_crises = ['CRI_001','CRI_002','CRI_003','CRI_004','CRI_005','CRI_006','CRI_007','CRI_008','CRI_009']
                public_cdata_keys = ['crisisId','name','kind','streetAddress','city','stateOrProvince','postalCode','country','dateAndTime','fatalities','injuries','populationIll','populationDisplaced','environmentalImpact','politicalChanges','culturalChanges','jobsLost','damageInUSD','reparationCost','regulatoryChanges']
                public_cdata = [{'crisisId':'CRI_001','name':'Haiti Earthquake','kind':'','streetAddress':'', 'city':'', 'stateOrProvince':'','postalCode':'','country':'','dateAndTime':'','fatalities':'0','injuries':'0','populationIll':'0','populationDisplaced':'0','environmentalImpact':'','politicalChanges':'','culturalChanges':'','jobsLost':'0','damageInUSD':'0','reparationCost':'0','regulatoryChanges':''},
                                {'crisisId':'CRI_002','name':'Hurricane Ike','kind':'','streetAddress':'', 'city':'', 'stateOrProvince':'','postalCode':'','country':'','dateAndTime':'','fatalities':'0','injuries':'0','populationIll':'0','populationDisplaced':'0','environmentalImpact':'','politicalChanges':'','culturalChanges':'','jobsLost':'0','damageInUSD':'0','reparationCost':'0','regulatoryChanges':''},
                                {'crisisId':'CRI_003','name':'Benghazi Attack','kind':'','streetAddress':'', 'city':'', 'stateOrProvince':'','postalCode':'','country':'','dateAndTime':'','fatalities':'0','injuries':'0','populationIll':'0','populationDisplaced':'0','environmentalImpact':'','politicalChanges':'','culturalChanges':'','jobsLost':'0','damageInUSD':'0','reparationCost':'0','regulatoryChanges':''},
                                {'crisisId':'CRI_004','name':'Eruption of Eyjafjallajokull','kind':'','streetAddress':'', 'city':'', 'stateOrProvince':'','postalCode':'','country':'','dateAndTime':'','fatalities':'0','injuries':'0','populationIll':'0','populationDisplaced':'0','environmentalImpact':'','politicalChanges':'','culturalChanges':'','jobsLost':'0','damageInUSD':'0','reparationCost':'0','regulatoryChanges':''},
                                {'crisisId':'CRI_005','name':'Cold War','kind':'','streetAddress':'', 'city':'', 'stateOrProvince':'','postalCode':'','country':'','dateAndTime':'','fatalities':'0','injuries':'0','populationIll':'0','populationDisplaced':'0','environmentalImpact':'','politicalChanges':'','culturalChanges':'','jobsLost':'0','damageInUSD':'0','reparationCost':'0','regulatoryChanges':''},
                                {'crisisId':'CRI_006','name':'UT Tower Shooter','kind':'','streetAddress':'', 'city':'', 'stateOrProvince':'','postalCode':'','country':'','dateAndTime':'','fatalities':'0','injuries':'0','populationIll':'0','populationDisplaced':'0','environmentalImpact':'','politicalChanges':'','culturalChanges':'','jobsLost':'0','damageInUSD':'0','reparationCost':'0','regulatoryChanges':''},
                                {'crisisId':'CRI_007','name':'Hindenburg Disaster','kind':'','streetAddress':'', 'city':'', 'stateOrProvince':'','postalCode':'','country':'','dateAndTime':'','fatalities':'0','injuries':'0','populationIll':'0','populationDisplaced':'0','environmentalImpact':'','politicalChanges':'','culturalChanges':'','jobsLost':'0','damageInUSD':'0','reparationCost':'0','regulatoryChanges':''},
                                {'crisisId':'CRI_008','name':'Global Warning','kind':'','streetAddress':'', 'city':'', 'stateOrProvince':'','postalCode':'','country':'','dateAndTime':'','fatalities':'0','injuries':'0','populationIll':'0','populationDisplaced':'0','environmentalImpact':'','politicalChanges':'','culturalChanges':'','jobsLost':'0','damageInUSD':'0','reparationCost':'0','regulatoryChanges':''},
                                {'crisisId':'CRI_009','name':'Arctic Vortex','kind':'','streetAddress':'', 'city':'', 'stateOrProvince':'','postalCode':'','country':'','dateAndTime':'','fatalities':'0','injuries':'0','populationIll':'0','populationDisplaced':'0','environmentalImpact':'','politicalChanges':'','culturalChanges':'','jobsLost':'0','damageInUSD':'0','reparationCost':'0','regulatoryChanges':''}]
                J = iter(table)
                assert (type(table) is Element)
                for j in range(len(list(table))):
                    datas = list()
                    tags = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(len(list(row))):
                        col = next(K)
                        datas.append(col.text)
                        tags.append(col.tag)

                # This will merge the data
                    if datas[0] in public_crises:
                        c_num = int(datas[0][-2:]) - 1
                        for count in range(len(datas)):
                            if (public_cdata[c_num][public_cdata_keys[count]] != str(datas[count])) \
                                and (str(datas[count]) not in ['NULL','None','Null','']) \
                                and (tags[count] == public_cdata_keys[count]) \
                                and (tags[count] not in ['fatalities', 'injuries', 'populationIll','populationDisplaced','jobsLost','damageInUSD','reparationCost']):
                                public_cdata[c_num][public_cdata_keys[count]] = public_cdata[c_num][public_cdata_keys[count]] + '; ' + str(datas[count])
                            elif ((public_cdata[c_num][public_cdata_keys[count]] != str(datas[count])) \
                                and (str(datas[count]) not in ['NULL','None','Null','']) \
                                and (tags[count] == public_cdata_keys[count]) \
                                and (tags[count] in ['fatalities', 'injuries', 'populationIll','populationDisplaced','jobsLost','damageInUSD','reparationCost'])):
                                public_cdata[c_num][public_cdata_keys[count]] = (int(datas[count]) + int(public_cdata[c_num][public_cdata_keys[count]])) / 2
                            elif ((public_cdata[c_num][public_cdata_keys[count]] != str(datas[count])) and (str(datas[count]) not in ['NULL','None','Null',''])):
                                tag_Number = count
                                while tags[count] !=  public_cdata_keys[tag_Number] and tag_Number < len(public_cdata_keys):
                                    tag_Number += 1
                                public_cdata[c_num][public_cdata_keys[tag_Number]] = public_cdata[c_num][public_cdata_keys[tag_Number]] + '; ' + str(datas[count])
                    else:
                        tags_list = {0:'crisisId',1:'name',2:'kind',3:'streetAddress', 4: 'city', 5: 'stateOrProvince', 6: 'postalCode', 7:'country', 8:'dateAndTime',9:'fatalities', 10: 'injuries', 11: 'populationIll', 12:'populationDisplaced', 13:'environmentalImpact', 14: 'politicalChanges', 15: 'culturalChanges', 16: 'jobsLost', 17:'damageInUSD', 18: 'reparationCost', 19:'regulatoryChanges'} #complete this dictionary
                        
                        #Enter all the data if there is nothing missing
                        if len(datas) == 20:
                            query = 'insert into Crises values ("'+str(datas[0])+'","'+str(datas[1])+'","'+str(datas[2])+'","'+str(datas[3])+'","'
                            query = query +str(datas[4])+'","'+str(datas[5])+'","'+str(datas[6])+'","'+str(datas[7])+'","'+str(datas[8])+'","'
                            query = query +str(datas[9])+'","'+str(datas[10])+'","'+str(datas[11])+'","'+str(datas[12])+'","'+str(datas[13])+'","'
                            query = query +str(datas[14])+'","'+str(datas[15])+'","'+str(datas[16])+'","'+str(datas[17])+'","'+str(datas[18])+'","'+str(datas[19])+'"'
                         # Enters the "No Data" in table if something is missing
                        else:
                            query = 'insert into Crises values ("'
                            counter = 0
                            i = 0
                            while i < len(tags):
                                if tags_list[counter] == tags[i]:
                                    query += (str(datas[i])+'","')
                                    counter += 1
                                    i += 1
                                else:
                                    query += 'No data","'
                                    counter +=1
                            if len(tags) < 20:
                                for blanks in range(20-counter):
                                    query += 'No data","'
                            query = query[0:len(query)-2]
                        sql_query = sql_query + query + '); '
                        WCDB_query(c,query+'); ')
                        assert (type(sql_query) is str)
                                                                                                
            # Enters in the data to the Org table
            if(table.tag == 'orgs'):
                public_orgs = ['ORG_001','ORG_002','ORG_003','ORG_004','ORG_005','ORG_006','ORG_007','ORG_008','ORG_009','ORG_010','ORG_011','ORG_012','ORG_013']
                public_odata_keys = ['orgId','name','kind','streetAddress','city','stateOrProvince','postalCode','country','foundingMission','dateFounded','dateAbolished','majorEvents']
                public_odata = [{'orgId':'ORG_001','name':'UNICEF','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':'','foundingMission':'','dateFounded':'','dateAbolished':'','majorEvents':'',},
                                {'orgId':'ORG_002','name':'United Nations','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':'','foundingMission':'','dateFounded':'','dateAbolished':'','majorEvents':'',},
                                {'orgId':'ORG_003','name':'U.S. Coast Guard','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':'','foundingMission':'','dateFounded':'','dateAbolished':'','majorEvents':'',},
                                {'orgId':'ORG_004','name':'U.S. Marine Corps','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':'','foundingMission':'','dateFounded':'','dateAbolished':'','majorEvents':'',},
                                {'orgId':'ORG_005','name':'World Health Organization','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':'','foundingMission':'','dateFounded':'','dateAbolished':'','majorEvents':'',},
                                {'orgId':'ORG_006','name':'Haitian Government','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':'','foundingMission':'','dateFounded':'','dateAbolished':'','majorEvents':'',},
                                {'orgId':'ORG_007','name':'Luftschiffbau Zeppelin','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':'','foundingMission':'','dateFounded':'','dateAbolished':'','majorEvents':'',},
                                {'orgId':'ORG_008','name':'Austin Police Department','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':'','foundingMission':'','dateFounded':'','dateAbolished':'','majorEvents':'',},
                                {'orgId':'ORG_009','name':'U.S. Department of State','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':'','foundingMission':'','dateFounded':'','dateAbolished':'','majorEvents':'',},
                                {'orgId':'ORG_010','name':'International Red Cross','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':'','foundingMission':'','dateFounded':'','dateAbolished':'','majorEvents':'',},
                                {'orgId':'ORG_011','name':'Portlight Strategies, Inc.','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':'','foundingMission':'','dateFounded':'','dateAbolished':'','majorEvents':'',},
                                {'orgId':'ORG_012','name':'Homeland Security','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':'','foundingMission':'','dateFounded':'','dateAbolished':'','majorEvents':'',},
                                {'orgId':'ORG_013','name':'London Volcanic Ash Advisory Center','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':'','foundingMission':'','dateFounded':'','dateAbolished':'','majorEvents':'',}]
            

                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    tags = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(len(list(row))):
                        col = next(K)
                        datas.append(col.text)
                        tags.append(col.tag)

                    if datas[0] in public_orgs:
                        o_num = int(datas[0][-2:]) - 1
                        for count in range(len(datas)):
                            if (public_odata[o_num][public_odata_keys[count]] != str(datas[count])) and (str(datas[count]) not in ['NULL','None','Null','']) and (tags[count] == public_odata_keys[count]):
                                public_odata[o_num][public_odata_keys[count]] = public_odata[o_num][public_odata_keys[count]] + '; ' + str(datas[count])
                            elif ((public_odata[o_num][public_odata_keys[count]] != str(datas[count])) and (str(datas[count]) not in ['NULL','None','Null',''])):
                                tag_Number = count
                                while (tags[count] !=  public_odata_keys[tag_Number]) and (tag_Number < len(public_odata_keys)):
                                    tag_Number += 1
                                public_odata[o_num][public_odata_keys[tag_Number]] = public_odata[o_num][public_odata_keys[tag_Number]] + '; ' + str(datas[count])

                    else:
                        tags_list = {0:'orgId',1:'name',2:'kind',3:'streetAddress', 4: 'city', 5: 'stateOrProvince', 6: 'postalCode', 7:'country', 8:'foundingMission', 9:'dateFounded', 10:'dateAbolished', 11: 'majorEvents'} #complete this dictionary

                        if len(datas)== 12:
                            query = 'insert into Orgs values ("'+str(datas[0])+'","'+str(datas[1])+'","'+str(datas[2])+'","'+str(datas[3])+'","'
                            query = query +str(datas[4])+'","'+str(datas[5])+'","'+str(datas[6])+'","'+str(datas[7])+'","'+str(datas[8])+'","'
                            query = query +str(datas[9])+'","'+str(datas[10])+ '","'+str(datas[11])+'"'
                        else:
                            query = 'insert into Orgs values ("'
                            i = 0
                            counter = 0
                            while i < len(tags):
                                if tags_list[counter] == tags[i]:
                                    query += (str(datas[i])+'","')
                                    counter += 1
                                    i += 1
                                else:
                                    query += 'No data","'
                                    counter += 1
                            if len(tags) < 12:
                                for blanks in range(12-counter):
                                    query += 'No data","'
                            query = query[0:len(query)-2]
                        sql_query = sql_query + query + '); '
                        WCDB_query(c,query+'); ')
                        assert (type(sql_query) is str)

            # Enters in the data for the People table
            if(table.tag == 'people'):
                J = iter(table)
                public_people = ['PER_001','PER_002','PER_003','PER_004','PER_005','PER_006','PER_007','PER_008','PER_009','PER_010','PER_011','PER_012','PER_013']
                public_pdata_keys= ['personId','name','kind','streetAddress','city','stateOrProvince','postalCode','country']
                public_pdata = [{'personId':'PER_001','name':'Barack Obama','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':''},
                                {'personId':'PER_002','name':'Hillary Clinton','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':''},
                                {'personId':'PER_003','name':'Ben S. Bernanke','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':''},
                                {'personId':'PER_004','name':'Colin L. Powell','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':''},
                                {'personId':'PER_005','name':'Jean-Max Bellerive','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':''},
                                {'personId':'PER_006','name':'Olafur Ragnar Grimsson','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':''},
                                {'personId':'PER_007','name':'Ernst August Lehmann','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':''},
                                {'personId':'PER_008','name':'George W. Bush','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':''},
                                {'personId':'PER_009','name':'Charles Whitman','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':''},
                                {'personId':'PER_010','name':'Greg Steinhafel','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':''},
                                {'personId':'PER_011','name':'Bill White','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':''},
                                {'personId':'PER_012','name':'Michele Pierre Louis','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':''},
                                {'personId':'PER_013','name':'Anthony Lake','kind':'','streetAddress':'','city':'','stateOrProvince':'','postalCode':'','country':''}]
                for j in range(len(list(table))):
                    datas = list()
                    tags = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(len(list(row))):
                        col = next(K)
                        datas.append(col.text)
                        tags.append(col.tag)

                    if datas[0] in public_people:
                        p_num = int(datas[0][-2:]) - 1
                        for count in range(len(datas)):
                            if (public_pdata[p_num][public_pdata_keys[count]] != str(datas[count])) and (str(datas[count]) not in ['NULL','None','Null','']) and (tags[count] == public_pdata_keys[count]):
                                public_pdata[p_num][public_pdata_keys[count]] = public_pdata[p_num][public_pdata_keys[count]] + '; ' + str(datas[count])
                            elif ((public_pdata[p_num][public_pdata_keys[count]] != str(datas[count])) and (str(datas[count]) not in ['NULL','None','Null',''])):
                                tag_Number = count
                                while (tags[count] !=  public_pdata_keys[tag_Number]) and (tag_Number < len(public_pdata_keys)):
                                    tag_Number += 1
                                public_pdata[p_num][public_pdata_keys[tag_Number]] = public_pdata[p_num][public_pdata_keys[tag_Number]] + '; ' + str(datas[count])
                        
                    else:
                        tags_list = {0:'personId',1:'name',2:'kind',3:'streetAddress', 4: 'city', 5: 'stateOrProvince', 6: 'postalCode', 7:'country'} #complete this dictionary
                        if len(datas)== 8:
                            query = 'insert into People values ("'+str(datas[0])+'","'+str(datas[1])+'","'+str(datas[2])+'","'+str(datas[3])+'","'
                            query = query +str(datas[4])+'","'+str(datas[5])+'","'+str(datas[6])+'","'+str(datas[7])+'"'
                        else:
                            query = 'insert into People values ("'
                            i = 0
                            counter = 0
                            while i < len(tags):
                                if tags_list[counter] == tags[i]:
                                    query += (str(datas[i])+'","')
                                    counter += 1
                                    i += 1
                                else:
                                    query += 'No data","'
                                    counter += 1
                            if len(tags) < 8:
                                for blanks in range(8-counter):
                                    query += 'No data","'
                            query = query[0:len(query)-2]
                        sql_query = sql_query + query + '); '
                        WCDB_query(c,query+'); ')
                        assert (type(sql_query) is str)

            # Enters in the data for the Resources table
            if(table.tag == 'resources'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)

                    if str(datas[0]) not in RES_List:
                        RES_List.append(str(datas[0]))
                    else:
                        old_id = str(datas[0])
                        datas[0] = str(datas[0][0:3]) + str(z) + str(datas[0][-3:])
                        RES_List.append(str(datas[0]))
                        _set.append((old_id,str(datas[0])))

                    query = 'insert into Resources values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)

                z += 1

            # Enters in the data for the CrisisResources table
            if(table.tag == 'crisisResources'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)
                    query = 'insert into CrisisResources values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)

            # Enters in the data for the WaysToHelp table
            if(table.tag == 'waysToHelp'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)

                    if datas[0] not in WAY_List:
                        WAY_List.append(datas[0])
                    else:
                        old_id = datas[0]
                        datas[0] = datas[0][0:3] + str(y) + datas[0][-3:]
                        WAY_List.append(datas[0])
                        _set.append((old_id,datas[0]))


                    query = 'insert into WaysToHelp values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)

                y += 1

            # Enters in the data for the CrisisWaysToHelp table
            if(table.tag == 'crisisWaysToHelp'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)
                    query = 'insert into CrisisWaysToHelp values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
            
            # Enters in the data for the OrgContactInfos table
            if(table.tag == 'orgContactInfos'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)
                    query = 'insert into OrgContactInfos values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)

            # Enters in the data for the ContanctInfos table
            if(table.tag == 'contactInfos'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    tags = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(len(list(row))):
                        col = next(K)
                        datas.append(col.text)
                        tags.append(col.tag)

                    if datas[0] not in CON_List:
                        CON_List.append(datas[0])
                    else:
                        old_id = datas[0]
                        datas[0] = datas[0][0:3] + str(x) + datas[0][-3:]
                        CON_List.append(datas[0])
                        _set.append((old_id,datas[0]))


                    tags_list = {0:'contactInfoId',1:'phoneNumber',2:'emailAddress',3:'facebookUrlID', 4: 'twitterUrlID', 5: 'websiteUrlID'} #complete this dictionary
                    if len(datas) == 6:
                        query = 'insert into ContactInfos values ("'+str(datas[0])+'","'+str(datas[1])+'","'
                        query = query +str(datas[2])+'","'+str(datas[3])+'","' +str(datas[4])+'","'+str(datas[5])+'"'
                    else:
                        query = 'insert into ContactInfos values ("'
                        i = 0
                        counter = 0
                        while i < len(tags):
                            if tags_list[counter] == tags[i]:
                                query += (str(datas[i])+'","')
                                counter += 1
                                i += 1
                            else:
                                query += 'No data","'
                                counter += 1
                        if len(tags) < 6:
                            for blanks in range(6-counter):
                                query += 'No data","'
                        query = query[0:len(query)-2]
                    sql_query = sql_query + query + '); '
                    WCDB_query(c,query+'); ')
                    assert (type(sql_query) is str)
                
                x += 1
     
            # Enters in the data for the Citations table
            if(table.tag == 'citations'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)

                    

                    if datas[0] not in CIT_List:
                        CIT_List.append(datas[0])
                    else:
                        old_id = datas[0]
                        datas[0] = datas[0][0:3] + str(w) + datas[0][-3:]
                        CIT_List.append(datas[0])
                        _set.append((old_id,datas[0]))

                    query = 'insert into Citations values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)

                w += 1

            # Enters in the data for the CrisisCitations
            if(table.tag == 'crisisCitations'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)
                    query = 'insert into CrisisCitations values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)

             # Enters in the data for the OrgCitations tabl
            if(table.tag == 'orgCitations'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)
                    query = 'insert into OrgCitations values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)

            # Enters in the data for the PersonCitations table
            if(table.tag == 'personCitations'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)
                    query = 'insert into PersonCitations values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)

            # Enters in the data for the Urls table
            if(table.tag == 'urls'):

                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    tags = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(len(list(row))):
                        col = next(K)
                        datas.append(col.text)
                        tags.append(col.tag)

                    tags_list = {0:'urlID',1:'type',2:'urlAddress'} #complete this dictionary
                    
                    

                    if datas[0] not in url_List:
                        url_List.append(datas[0])
                    else:
                        old_id = datas[0]
                        datas[0] = datas[0][0:3] + str(v) + datas[0][-3:]
                        url_List.append(datas[0])
                        _set.append((old_id,datas[0]))

                    if len(datas) == 3:
                        query = 'insert into Urls values ("'+str(datas[0])+'","'+str(datas[1])+'","'+str(datas[2])+'"'
                    else:
                        query = 'insert into Urls values ('
                        i = 0
                        counter = 0
                        while i < len(tags):
                            if tags_list[counter] == tags[i]:
                                query += (str(datas[i])+'","')
                                counter += 1
                                i += 1
                            else:
                                query += 'None","'
                                counter += 1
                        if len(tags) < 3:
                            for blanks in range(counter):
                                query += 'No data","'
                        query = query[0:len(query)-2]
                    sql_query = sql_query + query + '); '
                    WCDB_query(c,query+'); ')
                    assert (type(sql_query) is str)

                v += 1
            
            # Enters in the data for the CrisisUrls table
            if(table.tag == 'crisisUrls'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)
                    query = 'insert into CrisisUrls values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)

            # Enters in the data for the OrgUrls
            if(table.tag == 'orgUrls'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)
                    query = 'insert into OrgUrls values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)
           
            # Enters in the data for the PersonUrls
            if(table.tag == 'personUrls'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)
                    query = 'insert into PersonUrls values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)

            # Enters in the data for the CrisisOrgs
            if(table.tag == 'crisisOrgs'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)
                    query = 'insert into CrisisOrgs values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)

            # Enters in the data for the CrisisPeople
            if(table.tag == 'crisisPeople'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)
                    query = 'insert into CrisisPeople values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)
                    
            # Enters in the data for the OrgPeople
            if(table.tag == 'orgPeople'):
                J = iter(table)
                for j in range(len(list(table))):
                    datas = list()
                    row = next(J)
                    K = iter(row)
                    for k in range(2):
                        col = next(K)
                        datas.append(col.text)
                    query = 'insert into OrgPeople values ("'+str(datas[0])+'","'+str(datas[1])+'")'
                    sql_query = sql_query + query + '; '
                    WCDB_query(c,query+'; ')
                    assert (type(sql_query) is str)
    return sql_query


def WCDB_export(c):
   """
   Export from database to xml
   """

   # Prepare a cursor object using cursor() method
   cursor = c.cursor()
   builder = TreeBuilder()

   crisis_tags = {0:'crisisId',1:'name',2:'kind',3:'streetAddress', 4: 'city', 5: 'stateOrProvince', 6: 'postalCode', 7:'country', 8:'dateAndTime',9:'fatalities', 10:'injuries', 11:'populationIll', 12:'populationDisplaced', 13:'environmentalImpact', 14:'politicalChanges', 15:'culturalChanges', 16:'jobsLost', 17:'damageInUSD', 18:'reparationCost', 19:'regulatoryChanges'}
   org_tags = {0:'orgId',1:'name',2:'kind',3:'streetAddress', 4: 'city', 5: 'stateOrProvince', 6: 'postalCode', 7:'country', 8:'foundingMission', 9:'dateFounded', 10:'dateAbolished', 11: 'majorEvents'}
   person_tags = {0:'personId',1:'name',2:'kind',3:'streetAddress', 4: 'city', 5: 'stateOrProvince', 6: 'postalCode', 7:'country'}
   resources_tags = {0:'resourceId', 1:'resource'}
   crisisResource_tags = {0:'crisisId', 1:'resourceId'}
   wayToHelp_tags = {0:'helpId', 1:'wayToHelp'}
   crisisWaysToHelp_tags = {0:'crisisId', 1:'helpId'}
   contact_tags = {0:'contactInfoId',1:'phoneNumber',2:'emailAddress',3:'facebookUrlID', 4: 'twitterUrlID', 5: 'websiteUrlID'}
   orgContactInfo_tags = {0:'orgId', 1:'contactInfoId'}
   citation_tags = {0:'citationId', 1:'citation'}
   crisisCitation_tags = {0:'citationId', 1:'crisisId'}
   orgCitation_tags = {0:'orgId', 1:'citationId'}
   personCitation_tags = {0:'personId', 1:'citationId'}
   url_tags = {0:'urlId', 1:'type', 2:'urlAddress'}
   crisisUrl_tags = {0:'crisisId', 1:'urlId'}
   orgUrl_tags = {0:'orgId', 1: 'urlId'}
   personUrl_tags = {0:'personId', 1:'urlId'}
   crisisOrg_tags = {0:'crisisId', 1:'orgId'}
   crisisPerson_tags = {0:'crisisId', 1:'personId'}
   orgPerson_tags = {0:'orgId', 1:'personId'}

   builder.start("root", {})

   # execute the SQL query using execute() method.
   cursor.execute("select * from Crises;")

   # fetch all of the rows from the query.
   data = cursor.fetchall()

   builder.start("crises", {})
   
   for rows in data:
       i = 0
       builder.start("crisis", {})
       for row in rows:
           builder.start(crisis_tags[i], {})
           builder.data(str(row))
           builder.end(crisis_tags[i])
           i += 1
       builder.end("crisis")

   builder.end("crises")

   cursor.execute("select * from Orgs;")
   data = cursor.fetchall()

   builder.start("orgs", {})

   for rows in data:
       i = 0
       builder.start("org", {})
       for row in rows:
           builder.start(org_tags[i], {})
           builder.data(str(row))
           builder.end(org_tags[i])
           i += 1
       builder.end("org")

   builder.end("orgs")

   cursor.execute("select * from People;")
   data = cursor.fetchall()

   builder.start("people", {})

   for rows in data:
       i = 0
       builder.start("person", {})
       for row in rows:
           builder.start(person_tags[i], {})
           builder.data(str(row))
           builder.end(person_tags[i])
           i += 1
       builder.end("person")

   builder.end("people")

   cursor.execute("select * from Resources;")
   data = cursor.fetchall()

   builder.start("resources", {})

   for rows in data:
       i = 0
       builder.start("resourcePair", {})
       for row in rows:
           builder.start(resources_tags[i], {})
           if (i == 0):
            if (row[3] != '_'):
                s = list(row)
                s[3]
                "".join(s)
           builder.data(str(row))
           builder.end(resources_tags[i])
           i += 1
       builder.end("resourcePair")

   builder.end("resources")

   cursor.execute("select * from CrisisResources;")
   data = cursor.fetchall()

   builder.start("crisisResources", {})

   for rows in data:
       i = 0
       builder.start("crisisResourcesPair", {})
       for row in rows:
               builder.start(crisisResource_tags[i], {})
               builder.data(str(row))
               builder.end(crisisResource_tags[i])
               i += 1
       builder.end("crisisResourcesPair")

   builder.end("crisisResources")

   cursor.execute("select * from WaysToHelp;")
   data = cursor.fetchall()

   builder.start("waysToHelp", {})

   for rows in data:
       i = 0
       builder.start("wayToHelpPair", {})
       for row in rows:
               builder.start(wayToHelp_tags[i], {})
               if (i == 0):
                if row[3] != '_' :
                    s = list(row)
                    s[3]
                    "".join(s)
               builder.data(str(row))
               builder.end(wayToHelp_tags[i])
               i += 1
       builder.end("wayToHelpPair")

   builder.end("waysToHelp")

   cursor.execute("select * from CrisisWaysToHelp;")
   data = cursor.fetchall()

   builder.start("crisisWaysToHelp", {})

   for rows in data:
       i = 0
       builder.start("crisisWayToHelpPair", {})
       for row in rows:
               builder.start(crisisWaysToHelp_tags[i], {})
               builder.data(str(row))    
               builder.end(crisisWaysToHelp_tags[i])
               i += 1
       builder.end("crisisWayToHelpPair")

   builder.end("crisisWaysToHelp")


   cursor.execute("select * from ContactInfos;")
   data = cursor.fetchall()

   builder.start("contactInfos", {})

   for rows in data:
       i = 0
       builder.start("contactInfo", {})
       for row in rows:
               builder.start(contact_tags[i], {})
               if (i == 0):
                if row[3] != '_':
                    s = list(row)
                    s[3]
                    "".join(s)
               builder.data(str(row))
               builder.end(contact_tags[i])
               i += 1
       builder.end("contactInfo")

   builder.end("contactInfos")

   cursor.execute("select * from OrgContactInfos;")
   data = cursor.fetchall()

   builder.start("orgContactInfos", {})

   for rows in data:
       i = 0
       builder.start("orgContactInfoPair", {})
       for row in rows:
               builder.start(orgContactInfo_tags[i], {})
               builder.data(str(row))
               builder.end(orgContactInfo_tags[i])
               i += 1
       builder.end("orgContactInfoPair")

   builder.end("orgContactInfos")

   cursor.execute("select * from Citations;")
   data = cursor.fetchall()

   builder.start("citations", {})

   for rows in data:
       i = 0
       builder.start("citationPair", {})
       for row in rows:
               builder.start(citation_tags[i], {})
               if (i == 0):
                if row[3] != '_':
                    s = list(row)
                    s[3]
                    "".join(s)
               builder.data(str(row))
               builder.end(citation_tags[i])
               i += 1
       builder.end("citationPair")

   builder.end("citations")

   cursor.execute("select * from CrisisCitations;")
   data = cursor.fetchall()

   builder.start("crisisCitations", {})

   for rows in data:
       i = 0
       builder.start("crisisCitationPair", {})
       for row in rows:
               builder.start(crisisCitation_tags[i], {})
               builder.data(str(row))
               builder.end(crisisCitation_tags[i])
               i += 1
       builder.end("crisisCitationPair")

   builder.end("crisisCitations")

   cursor.execute("select * from OrgCitations;")
   data = cursor.fetchall()

   builder.start("orgCitations", {})

   for rows in data:
       i = 0
       builder.start("orgCitationPair", {})
       for row in rows:
           builder.start(orgCitation_tags[i], {})
           builder.data(str(row))
           builder.end(orgCitation_tags[i])
           i += 1
       builder.end("orgCitationPair")

   builder.end("orgCitations")

   cursor.execute("select * from PersonCitations;")
   data = cursor.fetchall()

   builder.start("personCitations", {})

   for rows in data:
       i = 0
       builder.start("personCitationPair", {})
       for row in rows:
           builder.start(personCitation_tags[i], {})
           builder.data(str(row))
           builder.end(personCitation_tags[i])
           i += 1
       builder.end("personCitationPair")

   builder.end("personCitations")

   cursor.execute("select * from Urls;")
   data = cursor.fetchall()

   builder.start("urls", {})

   for rows in data:
       i = 0
       builder.start("url", {})
       for row in rows:
           builder.start(url_tags[i], {})
           if (i == 0):
            if (row[3] != '_'):
                s = list(row)
                s[3]
                "".join(s)
           builder.data(str(row))
           builder.end(url_tags[i])
           i += 1
       builder.end("url")

   builder.end("urls")


   cursor.execute("select * from CrisisUrls;")
   data = cursor.fetchall()

   builder.start("crisisUrls", {})

   for rows in data:
       i = 0
       builder.start("crisisUrlPair", {})
       for row in rows:
           builder.start(crisisUrl_tags[i], {})
           builder.data(str(row))
           builder.end(crisisUrl_tags[i])
           i += 1
       builder.end("crisisUrlPair")

   builder.end("crisisUrls")

   cursor.execute("select * from OrgUrls;")
   data = cursor.fetchall()

   builder.start("orgUrls", {})

   for rows in data:
       i = 0
       builder.start("orgUrlPair", {})
       for row in rows:
           builder.start(orgUrl_tags[i], {})
           builder.data(str(row))
           builder.end(orgUrl_tags[i])
           i += 1
       builder.end("orgUrlPair")

   builder.end("orgUrls")

   cursor.execute("select * from PersonUrls;")
   data = cursor.fetchall()

   builder.start("personUrls", {})

   for rows in data:
       i = 0
       builder.start("personUrlPair", {})
       for row in rows:
           builder.start(personUrl_tags[i], {})
           builder.data(str(row))
           builder.end(personUrl_tags[i])
           i += 1
       builder.end("personUrlPair")

   builder.end("personUrls")

   cursor.execute("select * from CrisisOrgs;")
   data = cursor.fetchall()

   builder.start("crisisOrgs", {})

   for rows in data:
       i = 0
       builder.start("crisisOrgPair", {})
       for row in rows:
           builder.start(crisisOrg_tags[i], {})
           builder.data(str(row))
           builder.end(crisisOrg_tags[i])
           i += 1
       builder.end("crisisOrgPair")

   builder.end("crisisOrgs")

   cursor.execute("select * from CrisisPeople;")
   data = cursor.fetchall()

   builder.start("crisisPeople", {})

   for rows in data:
       i = 0
       builder.start("crisisPersonPair", {})
       for row in rows:
           builder.start(crisisPerson_tags[i], {})
           builder.data(str(row))
           builder.end(crisisPerson_tags[i])
           i += 1
       builder.end("crisisPersonPair")

   builder.end("crisisPeople")

   cursor.execute("select * from OrgPeople;")
   data = cursor.fetchall()

   builder.start("orgPeople", {})

   for rows in data:
       i = 0
       builder.start("orgPersonPair", {})
       for row in rows:
           builder.start(resources_tags[i], {})
           builder.data(str(row))
           builder.end(resources_tags[i])
           i += 1
       builder.end("orgPersonPair")

   builder.end("orgPeople")

   builder.end("root")

   xml = builder.close()

   # close the cursor object
   cursor.close()

   # close the connection
   c.close()

   string = tostring(xml)
   return string

    

def WCDB_solve(r,w):
    """
Solve is a helper function that calls import and export
to manipulate xml documents. r is a reader and w is a writer
It will take in an XML element, returning the same Element.
It will also populate the database.
"""
    nodes = r.read()
    c = WCDB_login() 
    WCDB_create(c)
    string = WCDB_import(nodes,c)
    xml = WCDB_export(c)
    w.write(unicode(xml))

