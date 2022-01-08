
public abstract class Station
{
    /** structure du fichier
     *
     * | offset | taille |exemple |signification
     * |--------|--------|--------|-------------
     * |      0 |      6 |225730  |USAF = Air Force Datsav3 station number
     * |      7 |      5 |99999   |WBAN = NCDC WBAN number
     * |     13 |     29 |LANNION |Station name
     * |     43 |      2 |US      |FIPS country ID (pays)
     * |     48 |      2 |KS      |ST = State for US stations
     * |     51 |      4 |LFRO    |CALL = ICAO call sign (code aéroport)
     * |     57 |      7 |+64.900 |LAT = Latitude in decimal degrees
     * |     65 |      8 |+045.767|LON = Longitude in decimal degrees
     * |     74 |      7 |+0071.0 |ELEV = Elevation in meters (altitude)
     * |     82 |      8 |19590101|BEGIN = Beginning Period Of Record (YYYYMMDD)
     * |     91 |      8 |20140206|END = Ending Period Of Record (YYYYMMDD)

     */
    static String ligne;

    public static void fromLine(String ligne)
    {
        Station.ligne = ligne;

    }


    public static String getNom() throws IndexOutOfBoundsException
    {
        return ligne.substring(13, 13+29);
    }
public static String getPaay() throws IndexOutOfBoundsException
    {
        return ligne.substring(0, 2);
    }
    public static double getCount() throws IndexOutOfBoundsException, NumberFormatException
    {
        return Double.parseDouble(ligne.substring(3,ligne.length()));
    }
    public static String getPays() throws IndexOutOfBoundsException
    {
        return ligne.substring(43, 43+2);
    }

    public static double getAltitude() throws IndexOutOfBoundsException, NumberFormatException
    {
        return Double.parseDouble(ligne.substring(57,57+7));
    }

    public static double getLongitude() throws IndexOutOfBoundsException, NumberFormatException
    {
        return Double.parseDouble(ligne.substring(65, 65+8));
    }
}
