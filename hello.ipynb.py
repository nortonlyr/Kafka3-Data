{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 66,
   "metadata": {},
   "outputs": [],
   "source": [
    "import numpy\n",
    "import pandas as pd"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 67,
   "metadata": {},
   "outputs": [],
   "source": [
    "nyc_park_df=pd.read_csv('/Users/nli/dev/airflow_home/data/nyc_park_data.csv')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 68,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "(2015, 36)"
      ]
     },
     "execution_count": 68,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "nyc_park_df.shape"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 69,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>the_geom</th>\n",
       "      <th>GISPROPNUM</th>\n",
       "      <th>OBJECTID</th>\n",
       "      <th>OMPPROPID</th>\n",
       "      <th>DEPARTMENT</th>\n",
       "      <th>PERMITDIST</th>\n",
       "      <th>PERMITPARE</th>\n",
       "      <th>PARENTID</th>\n",
       "      <th>LOCATION</th>\n",
       "      <th>COMMUNITYB</th>\n",
       "      <th>...</th>\n",
       "      <th>PERMIT</th>\n",
       "      <th>SIGNNAME</th>\n",
       "      <th>SUBCATEGOR</th>\n",
       "      <th>TYPECATEGO</th>\n",
       "      <th>URL</th>\n",
       "      <th>WATERFRONT</th>\n",
       "      <th>NYS_ASSEMB</th>\n",
       "      <th>NYS_SENATE</th>\n",
       "      <th>US_CONGRES</th>\n",
       "      <th>GlobalID</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>MULTIPOLYGON (((-74.14227319856263 40.54219614...</td>\n",
       "      <td>R145</td>\n",
       "      <td>6367</td>\n",
       "      <td>R145</td>\n",
       "      <td>R-03</td>\n",
       "      <td>R-03</td>\n",
       "      <td>R-03</td>\n",
       "      <td>R-03</td>\n",
       "      <td>Nelson Ave., Tennyson Dr. and Bulkhead Line</td>\n",
       "      <td>503</td>\n",
       "      <td>...</td>\n",
       "      <td>Y</td>\n",
       "      <td>Seaside Wildlife Nature Park</td>\n",
       "      <td>Neighborhood Park</td>\n",
       "      <td>Neighborhood Park</td>\n",
       "      <td>http://www.nycgovparks.org/parks/R145/</td>\n",
       "      <td>Yes</td>\n",
       "      <td>64</td>\n",
       "      <td>24</td>\n",
       "      <td>11</td>\n",
       "      <td>{EC14E5C9-9687-49BC-9A7A-77F977DC0448}</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>MULTIPOLYGON (((-73.90748556731788 40.75708917...</td>\n",
       "      <td>Q355</td>\n",
       "      <td>5916</td>\n",
       "      <td>Q355</td>\n",
       "      <td>Q-01</td>\n",
       "      <td>Q-01</td>\n",
       "      <td>Q-01</td>\n",
       "      <td>Q-01</td>\n",
       "      <td>31 Ave., 51 St., 54 St.</td>\n",
       "      <td>401</td>\n",
       "      <td>...</td>\n",
       "      <td>Y</td>\n",
       "      <td>Strippoli Square</td>\n",
       "      <td>Sitting Area/Triangle/Mall</td>\n",
       "      <td>Triangle/Plaza</td>\n",
       "      <td>http://www.nycgovparks.org/parks/Q355/</td>\n",
       "      <td>No</td>\n",
       "      <td>30</td>\n",
       "      <td>12</td>\n",
       "      <td>14</td>\n",
       "      <td>{62700020-4840-4F4A-A15A-7D65B9A6A794}</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>MULTIPOLYGON (((-74.00468042094083 40.65584018...</td>\n",
       "      <td>B210B</td>\n",
       "      <td>5465</td>\n",
       "      <td>B210B</td>\n",
       "      <td>B-07</td>\n",
       "      <td>B-07</td>\n",
       "      <td>B-07</td>\n",
       "      <td>B-07</td>\n",
       "      <td>3 Ave. bet. 35 St. and 34 St.</td>\n",
       "      <td>307</td>\n",
       "      <td>...</td>\n",
       "      <td>Y</td>\n",
       "      <td>D'Emic Playground</td>\n",
       "      <td>Neighborhood Plgd</td>\n",
       "      <td>Playground</td>\n",
       "      <td>http://www.nycgovparks.org/parks/B210B/</td>\n",
       "      <td>No</td>\n",
       "      <td>51</td>\n",
       "      <td>25</td>\n",
       "      <td>7</td>\n",
       "      <td>{BFD91324-49C1-46B5-B3E9-E43A989DC40B}</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>MULTIPOLYGON (((-73.85639866166588 40.80924557...</td>\n",
       "      <td>X262</td>\n",
       "      <td>4857</td>\n",
       "      <td>X262</td>\n",
       "      <td>X-09</td>\n",
       "      <td>X-09</td>\n",
       "      <td>X-09</td>\n",
       "      <td>X-09</td>\n",
       "      <td>Bolton Ave. bet. O'Brien Ave. and G St.</td>\n",
       "      <td>209</td>\n",
       "      <td>...</td>\n",
       "      <td>Y</td>\n",
       "      <td>Harding Park</td>\n",
       "      <td>Neighborhood Plgd</td>\n",
       "      <td>Neighborhood Park</td>\n",
       "      <td>http://www.nycgovparks.org/parks/X262/</td>\n",
       "      <td>No</td>\n",
       "      <td>85</td>\n",
       "      <td>34</td>\n",
       "      <td>15</td>\n",
       "      <td>{BDFFC8B5-573A-4771-8E51-601D03705C78}</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>MULTIPOLYGON (((-73.85415528855233 40.90187445...</td>\n",
       "      <td>X188</td>\n",
       "      <td>6621</td>\n",
       "      <td>X188</td>\n",
       "      <td>X-12</td>\n",
       "      <td>X-12</td>\n",
       "      <td>X-12</td>\n",
       "      <td>X-12</td>\n",
       "      <td>Matilda Ave. to Carpenter Ave. bet. E. 239 St....</td>\n",
       "      <td>212</td>\n",
       "      <td>...</td>\n",
       "      <td>Y</td>\n",
       "      <td>Wakefield Playground</td>\n",
       "      <td>JOP</td>\n",
       "      <td>Jointly Operated Playground</td>\n",
       "      <td>http://www.nycgovparks.org/parks/X188/</td>\n",
       "      <td>No</td>\n",
       "      <td>81</td>\n",
       "      <td>36</td>\n",
       "      <td>16</td>\n",
       "      <td>{E33C24CE-ACEB-4018-BAF0-3CE447C8A2AF}</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "<p>5 rows × 36 columns</p>\n",
       "</div>"
      ],
      "text/plain": [
       "                                            the_geom GISPROPNUM  OBJECTID  \\\n",
       "0  MULTIPOLYGON (((-74.14227319856263 40.54219614...       R145      6367   \n",
       "1  MULTIPOLYGON (((-73.90748556731788 40.75708917...       Q355      5916   \n",
       "2  MULTIPOLYGON (((-74.00468042094083 40.65584018...      B210B      5465   \n",
       "3  MULTIPOLYGON (((-73.85639866166588 40.80924557...       X262      4857   \n",
       "4  MULTIPOLYGON (((-73.85415528855233 40.90187445...       X188      6621   \n",
       "\n",
       "  OMPPROPID DEPARTMENT PERMITDIST PERMITPARE PARENTID  \\\n",
       "0      R145       R-03       R-03       R-03     R-03   \n",
       "1      Q355       Q-01       Q-01       Q-01     Q-01   \n",
       "2     B210B       B-07       B-07       B-07     B-07   \n",
       "3      X262       X-09       X-09       X-09     X-09   \n",
       "4      X188       X-12       X-12       X-12     X-12   \n",
       "\n",
       "                                            LOCATION COMMUNITYB  ... PERMIT  \\\n",
       "0        Nelson Ave., Tennyson Dr. and Bulkhead Line        503  ...      Y   \n",
       "1                            31 Ave., 51 St., 54 St.        401  ...      Y   \n",
       "2                      3 Ave. bet. 35 St. and 34 St.        307  ...      Y   \n",
       "3            Bolton Ave. bet. O'Brien Ave. and G St.        209  ...      Y   \n",
       "4  Matilda Ave. to Carpenter Ave. bet. E. 239 St....        212  ...      Y   \n",
       "\n",
       "                       SIGNNAME                  SUBCATEGOR  \\\n",
       "0  Seaside Wildlife Nature Park           Neighborhood Park   \n",
       "1              Strippoli Square  Sitting Area/Triangle/Mall   \n",
       "2             D'Emic Playground           Neighborhood Plgd   \n",
       "3                  Harding Park           Neighborhood Plgd   \n",
       "4          Wakefield Playground                         JOP   \n",
       "\n",
       "                    TYPECATEGO                                      URL  \\\n",
       "0            Neighborhood Park   http://www.nycgovparks.org/parks/R145/   \n",
       "1               Triangle/Plaza   http://www.nycgovparks.org/parks/Q355/   \n",
       "2                   Playground  http://www.nycgovparks.org/parks/B210B/   \n",
       "3            Neighborhood Park   http://www.nycgovparks.org/parks/X262/   \n",
       "4  Jointly Operated Playground   http://www.nycgovparks.org/parks/X188/   \n",
       "\n",
       "   WATERFRONT NYS_ASSEMB NYS_SENATE  US_CONGRES  \\\n",
       "0         Yes         64         24          11   \n",
       "1          No         30         12          14   \n",
       "2          No         51         25           7   \n",
       "3          No         85         34          15   \n",
       "4          No         81         36          16   \n",
       "\n",
       "                                 GlobalID  \n",
       "0  {EC14E5C9-9687-49BC-9A7A-77F977DC0448}  \n",
       "1  {62700020-4840-4F4A-A15A-7D65B9A6A794}  \n",
       "2  {BFD91324-49C1-46B5-B3E9-E43A989DC40B}  \n",
       "3  {BDFFC8B5-573A-4771-8E51-601D03705C78}  \n",
       "4  {E33C24CE-ACEB-4018-BAF0-3CE447C8A2AF}  \n",
       "\n",
       "[5 rows x 36 columns]"
      ]
     },
     "execution_count": 69,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "nyc_park_df.head()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 70,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>the_geom</th>\n",
       "      <th>GISPROPNUM</th>\n",
       "      <th>OBJECTID</th>\n",
       "      <th>OMPPROPID</th>\n",
       "      <th>DEPARTMENT</th>\n",
       "      <th>PERMITDIST</th>\n",
       "      <th>PERMITPARE</th>\n",
       "      <th>PARENTID</th>\n",
       "      <th>LOCATION</th>\n",
       "      <th>COMMUNITYB</th>\n",
       "      <th>...</th>\n",
       "      <th>PERMIT</th>\n",
       "      <th>SIGNNAME</th>\n",
       "      <th>SUBCATEGOR</th>\n",
       "      <th>TYPECATEGO</th>\n",
       "      <th>URL</th>\n",
       "      <th>WATERFRONT</th>\n",
       "      <th>NYS_ASSEMB</th>\n",
       "      <th>NYS_SENATE</th>\n",
       "      <th>US_CONGRES</th>\n",
       "      <th>GlobalID</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>2010</th>\n",
       "      <td>MULTIPOLYGON (((-73.95042032246141 40.80774069...</td>\n",
       "      <td>M351</td>\n",
       "      <td>87744</td>\n",
       "      <td>M351</td>\n",
       "      <td>M-10</td>\n",
       "      <td>M-10</td>\n",
       "      <td>M-10</td>\n",
       "      <td>M-10</td>\n",
       "      <td>W. 122 St. bet. Fred Douglass Blvd. and Adam C...</td>\n",
       "      <td>110</td>\n",
       "      <td>...</td>\n",
       "      <td>N</td>\n",
       "      <td>Joseph Daniel Wilson Garden</td>\n",
       "      <td>Greenthumb</td>\n",
       "      <td>Garden</td>\n",
       "      <td>http://www.nycgovparks.org/parks/M351/</td>\n",
       "      <td>No</td>\n",
       "      <td>70</td>\n",
       "      <td>30</td>\n",
       "      <td>13</td>\n",
       "      <td>{2C315CFE-7DD4-499A-A7EC-94C71E8277CD}</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2011</th>\n",
       "      <td>MULTIPOLYGON (((-73.99773025583967 40.72910998...</td>\n",
       "      <td>M395</td>\n",
       "      <td>6301</td>\n",
       "      <td>M395</td>\n",
       "      <td>M-02</td>\n",
       "      <td>M-02</td>\n",
       "      <td>M-02</td>\n",
       "      <td>M-02</td>\n",
       "      <td>La Guardia Pl. bet. W. 3 St. and Bleecker St.</td>\n",
       "      <td>102</td>\n",
       "      <td>...</td>\n",
       "      <td>NaN</td>\n",
       "      <td>Fiorello La Guardia Park</td>\n",
       "      <td>Neighborhood Plgd</td>\n",
       "      <td>Neighborhood Park</td>\n",
       "      <td>NaN</td>\n",
       "      <td>No</td>\n",
       "      <td>66</td>\n",
       "      <td>27</td>\n",
       "      <td>10</td>\n",
       "      <td>{14CA2496-AE32-4DB4-B89B-789B150B99B8}</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2012</th>\n",
       "      <td>MULTIPOLYGON (((-73.92392805010722 40.73250227...</td>\n",
       "      <td>Q360A</td>\n",
       "      <td>5693</td>\n",
       "      <td>Q360A</td>\n",
       "      <td>Q-02</td>\n",
       "      <td>Q-02</td>\n",
       "      <td>Q-02</td>\n",
       "      <td>Q-02</td>\n",
       "      <td>53 Ave.bet. 43 St. and 44 St.</td>\n",
       "      <td>402</td>\n",
       "      <td>...</td>\n",
       "      <td>N</td>\n",
       "      <td>Park</td>\n",
       "      <td>Sitting Area/Triangle/Mall</td>\n",
       "      <td>Strip</td>\n",
       "      <td>http://www.nycgovparks.org/parks/Q360A/</td>\n",
       "      <td>No</td>\n",
       "      <td>37</td>\n",
       "      <td>12</td>\n",
       "      <td>12</td>\n",
       "      <td>{CF96FDB1-B3AC-46FF-BE07-6110E5A53DF8}</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2013</th>\n",
       "      <td>MULTIPOLYGON (((-73.94567279852019 40.69436616...</td>\n",
       "      <td>B439</td>\n",
       "      <td>5242</td>\n",
       "      <td>B439</td>\n",
       "      <td>B-03</td>\n",
       "      <td>B-03</td>\n",
       "      <td>B-03</td>\n",
       "      <td>B-03</td>\n",
       "      <td>Tompkins Ave. and Willoughby Ave.</td>\n",
       "      <td>303</td>\n",
       "      <td>...</td>\n",
       "      <td>N</td>\n",
       "      <td>All People's Church of the Apostolic Faith Com...</td>\n",
       "      <td>Greenthumb</td>\n",
       "      <td>Garden</td>\n",
       "      <td>http://www.nycgovparks.org/parks/B439/</td>\n",
       "      <td>No</td>\n",
       "      <td>56</td>\n",
       "      <td>18</td>\n",
       "      <td>8</td>\n",
       "      <td>{B25B99C4-A478-482C-907C-BDBF453A9400}</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2014</th>\n",
       "      <td>MULTIPOLYGON (((-73.9053260213287 40.852331861...</td>\n",
       "      <td>X274</td>\n",
       "      <td>4791</td>\n",
       "      <td>X274</td>\n",
       "      <td>X-05</td>\n",
       "      <td>X-05</td>\n",
       "      <td>X-05</td>\n",
       "      <td>X-05</td>\n",
       "      <td>Creston Ave. bet. E. Burnside Ave. and E</td>\n",
       "      <td>205</td>\n",
       "      <td>...</td>\n",
       "      <td>Y</td>\n",
       "      <td>Mount Hope Garden</td>\n",
       "      <td>Neighborhood Plgd</td>\n",
       "      <td>Playground</td>\n",
       "      <td>http://www.nycgovparks.org/parks/X274/</td>\n",
       "      <td>No</td>\n",
       "      <td>86</td>\n",
       "      <td>33</td>\n",
       "      <td>15</td>\n",
       "      <td>{CEB0E5E5-FF85-4555-9E8E-A63AB2D2E71B}</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "<p>5 rows × 36 columns</p>\n",
       "</div>"
      ],
      "text/plain": [
       "                                               the_geom GISPROPNUM  OBJECTID  \\\n",
       "2010  MULTIPOLYGON (((-73.95042032246141 40.80774069...       M351     87744   \n",
       "2011  MULTIPOLYGON (((-73.99773025583967 40.72910998...       M395      6301   \n",
       "2012  MULTIPOLYGON (((-73.92392805010722 40.73250227...      Q360A      5693   \n",
       "2013  MULTIPOLYGON (((-73.94567279852019 40.69436616...       B439      5242   \n",
       "2014  MULTIPOLYGON (((-73.9053260213287 40.852331861...       X274      4791   \n",
       "\n",
       "     OMPPROPID DEPARTMENT PERMITDIST PERMITPARE PARENTID  \\\n",
       "2010      M351       M-10       M-10       M-10     M-10   \n",
       "2011      M395       M-02       M-02       M-02     M-02   \n",
       "2012     Q360A       Q-02       Q-02       Q-02     Q-02   \n",
       "2013      B439       B-03       B-03       B-03     B-03   \n",
       "2014      X274       X-05       X-05       X-05     X-05   \n",
       "\n",
       "                                               LOCATION COMMUNITYB  ...  \\\n",
       "2010  W. 122 St. bet. Fred Douglass Blvd. and Adam C...        110  ...   \n",
       "2011      La Guardia Pl. bet. W. 3 St. and Bleecker St.        102  ...   \n",
       "2012                      53 Ave.bet. 43 St. and 44 St.        402  ...   \n",
       "2013                  Tompkins Ave. and Willoughby Ave.        303  ...   \n",
       "2014           Creston Ave. bet. E. Burnside Ave. and E        205  ...   \n",
       "\n",
       "     PERMIT                                           SIGNNAME  \\\n",
       "2010      N                        Joseph Daniel Wilson Garden   \n",
       "2011    NaN                           Fiorello La Guardia Park   \n",
       "2012      N                                               Park   \n",
       "2013      N  All People's Church of the Apostolic Faith Com...   \n",
       "2014      Y                                  Mount Hope Garden   \n",
       "\n",
       "                      SUBCATEGOR         TYPECATEGO  \\\n",
       "2010                  Greenthumb             Garden   \n",
       "2011           Neighborhood Plgd  Neighborhood Park   \n",
       "2012  Sitting Area/Triangle/Mall              Strip   \n",
       "2013                  Greenthumb             Garden   \n",
       "2014           Neighborhood Plgd         Playground   \n",
       "\n",
       "                                          URL  WATERFRONT NYS_ASSEMB  \\\n",
       "2010   http://www.nycgovparks.org/parks/M351/          No         70   \n",
       "2011                                      NaN          No         66   \n",
       "2012  http://www.nycgovparks.org/parks/Q360A/          No         37   \n",
       "2013   http://www.nycgovparks.org/parks/B439/          No         56   \n",
       "2014   http://www.nycgovparks.org/parks/X274/          No         86   \n",
       "\n",
       "     NYS_SENATE  US_CONGRES                                GlobalID  \n",
       "2010         30          13  {2C315CFE-7DD4-499A-A7EC-94C71E8277CD}  \n",
       "2011         27          10  {14CA2496-AE32-4DB4-B89B-789B150B99B8}  \n",
       "2012         12          12  {CF96FDB1-B3AC-46FF-BE07-6110E5A53DF8}  \n",
       "2013         18           8  {B25B99C4-A478-482C-907C-BDBF453A9400}  \n",
       "2014         33          15  {CEB0E5E5-FF85-4555-9E8E-A63AB2D2E71B}  \n",
       "\n",
       "[5 rows x 36 columns]"
      ]
     },
     "execution_count": 70,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "nyc_park_df.tail()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 71,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "the_geom      2015\n",
       "GISPROPNUM    2015\n",
       "OBJECTID      2015\n",
       "OMPPROPID     2015\n",
       "DEPARTMENT    2015\n",
       "PERMITDIST    1969\n",
       "PERMITPARE    1967\n",
       "PARENTID      2012\n",
       "LOCATION      2015\n",
       "COMMUNITYB    2015\n",
       "COUNCILDIS    2014\n",
       "PRECINCT      1978\n",
       "ZIPCODE       2013\n",
       "BOROUGH       2015\n",
       "ACRES         2015\n",
       "RETIRED       2015\n",
       "EAPPLY        1930\n",
       "PIP_RATABL    1967\n",
       "GISOBJID      2015\n",
       "CLASS         2015\n",
       "COMMISSION    1834\n",
       "ACQUISITIO    1929\n",
       "ADDRESS       1080\n",
       "JURISDICTI    2015\n",
       "MAPPED        2009\n",
       "NAME311       2014\n",
       "PERMIT        1911\n",
       "SIGNNAME      2015\n",
       "SUBCATEGOR    1876\n",
       "TYPECATEGO    2015\n",
       "URL           1905\n",
       "WATERFRONT    2015\n",
       "NYS_ASSEMB    2015\n",
       "NYS_SENATE    2014\n",
       "US_CONGRES    2014\n",
       "GlobalID      2015\n",
       "dtype: int64"
      ]
     },
     "execution_count": 71,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "nyc_park_df.count()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 72,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "the_geom        0\n",
       "GISPROPNUM      0\n",
       "OBJECTID        0\n",
       "OMPPROPID       0\n",
       "DEPARTMENT      0\n",
       "PERMITDIST     46\n",
       "PERMITPARE     48\n",
       "PARENTID        3\n",
       "LOCATION        0\n",
       "COMMUNITYB      0\n",
       "COUNCILDIS      1\n",
       "PRECINCT       37\n",
       "ZIPCODE         2\n",
       "BOROUGH         0\n",
       "ACRES           0\n",
       "RETIRED         0\n",
       "EAPPLY         85\n",
       "PIP_RATABL     48\n",
       "GISOBJID        0\n",
       "CLASS           0\n",
       "COMMISSION    181\n",
       "ACQUISITIO     86\n",
       "ADDRESS       935\n",
       "JURISDICTI      0\n",
       "MAPPED          6\n",
       "NAME311         1\n",
       "PERMIT        104\n",
       "SIGNNAME        0\n",
       "SUBCATEGOR    139\n",
       "TYPECATEGO      0\n",
       "URL           110\n",
       "WATERFRONT      0\n",
       "NYS_ASSEMB      0\n",
       "NYS_SENATE      1\n",
       "US_CONGRES      1\n",
       "GlobalID        0\n",
       "dtype: int64"
      ]
     },
     "execution_count": 72,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "nyc_park_df.isnull().sum()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 73,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "Index(['the_geom', 'GISPROPNUM', 'OBJECTID', 'OMPPROPID', 'DEPARTMENT',\n",
       "       'PERMITDIST', 'PERMITPARE', 'PARENTID', 'LOCATION', 'COMMUNITYB',\n",
       "       'COUNCILDIS', 'PRECINCT', 'ZIPCODE', 'BOROUGH', 'ACRES', 'RETIRED',\n",
       "       'EAPPLY', 'PIP_RATABL', 'GISOBJID', 'CLASS', 'COMMISSION', 'ACQUISITIO',\n",
       "       'ADDRESS', 'JURISDICTI', 'MAPPED', 'NAME311', 'PERMIT', 'SIGNNAME',\n",
       "       'SUBCATEGOR', 'TYPECATEGO', 'URL', 'WATERFRONT', 'NYS_ASSEMB',\n",
       "       'NYS_SENATE', 'US_CONGRES', 'GlobalID'],\n",
       "      dtype='object')"
      ]
     },
     "execution_count": 73,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "nyc_park_df.columns"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 74,
   "metadata": {},
   "outputs": [],
   "source": [
    "nyc_park_df2 = nyc_park_df.drop(columns=['the_geom', 'GISPROPNUM', 'OBJECTID', 'OMPPROPID', 'DEPARTMENT',\n",
    "       'PERMITDIST', 'PERMITPARE', 'PARENTID', 'LOCATION','COMMUNITYB','COUNCILDIS', 'PRECINCT', 'ZIPCODE','RETIRED',\n",
    "       'EAPPLY', 'PIP_RATABL', 'GISOBJID', 'CLASS', 'COMMISSION', 'ACQUISITIO', 'ADDRESS', 'JURISDICTI', 'MAPPED', \n",
    "        'NAME311', 'PERMIT', 'SIGNNAME','SUBCATEGOR', 'URL','NYS_ASSEMB','NYS_SENATE', 'US_CONGRES', 'GlobalID'])"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 75,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>BOROUGH</th>\n",
       "      <th>ACRES</th>\n",
       "      <th>TYPECATEGO</th>\n",
       "      <th>WATERFRONT</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>R</td>\n",
       "      <td>20.907</td>\n",
       "      <td>Neighborhood Park</td>\n",
       "      <td>Yes</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>Q</td>\n",
       "      <td>0.061</td>\n",
       "      <td>Triangle/Plaza</td>\n",
       "      <td>No</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>B</td>\n",
       "      <td>1.130</td>\n",
       "      <td>Playground</td>\n",
       "      <td>No</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>X</td>\n",
       "      <td>2.160</td>\n",
       "      <td>Neighborhood Park</td>\n",
       "      <td>No</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>X</td>\n",
       "      <td>1.104</td>\n",
       "      <td>Jointly Operated Playground</td>\n",
       "      <td>No</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "  BOROUGH   ACRES                   TYPECATEGO WATERFRONT\n",
       "0       R  20.907            Neighborhood Park        Yes\n",
       "1       Q   0.061               Triangle/Plaza         No\n",
       "2       B   1.130                   Playground         No\n",
       "3       X   2.160            Neighborhood Park         No\n",
       "4       X   1.104  Jointly Operated Playground         No"
      ]
     },
     "execution_count": 75,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "nyc_park_df2.head()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 76,
   "metadata": {},
   "outputs": [],
   "source": [
    "nyc_park_df2.columns = ['borough', 'acres', 'typecatego', 'waterfront']"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 77,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>borough</th>\n",
       "      <th>acres</th>\n",
       "      <th>typecatego</th>\n",
       "      <th>waterfront</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>R</td>\n",
       "      <td>20.907</td>\n",
       "      <td>Neighborhood Park</td>\n",
       "      <td>Yes</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>Q</td>\n",
       "      <td>0.061</td>\n",
       "      <td>Triangle/Plaza</td>\n",
       "      <td>No</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>B</td>\n",
       "      <td>1.130</td>\n",
       "      <td>Playground</td>\n",
       "      <td>No</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>X</td>\n",
       "      <td>2.160</td>\n",
       "      <td>Neighborhood Park</td>\n",
       "      <td>No</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>X</td>\n",
       "      <td>1.104</td>\n",
       "      <td>Jointly Operated Playground</td>\n",
       "      <td>No</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "  borough   acres                   typecatego waterfront\n",
       "0       R  20.907            Neighborhood Park        Yes\n",
       "1       Q   0.061               Triangle/Plaza         No\n",
       "2       B   1.130                   Playground         No\n",
       "3       X   2.160            Neighborhood Park         No\n",
       "4       X   1.104  Jointly Operated Playground         No"
      ]
     },
     "execution_count": 77,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "nyc_park_df2.head()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 78,
   "metadata": {},
   "outputs": [],
   "source": [
    "nyc_park_df2.set_index('borough', inplace=True)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 80,
   "metadata": {},
   "outputs": [],
   "source": [
    "nyc_park_df2.to_csv('/Users/nli/dev/airflow_home/data/nyc_park_data2.csv')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.7.4"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}

