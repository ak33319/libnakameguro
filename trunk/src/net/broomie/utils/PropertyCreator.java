/**
 * PropertyCreator - the class for countint words from japanese doc.
 */

package net.broomie;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertyCreator {
    public PropertyCreator() {
        Properties prop = new Properties();
        prop.setProperty("libnakameguro.senConfPath", "/usr/local/sen/conf/sen.xml");
        prop.setProperty("libnakameguro.dfdbPath", "/user/kimura/wiki_countl.txt");
        try {
            OutputStream stream = new FileOutputStream("libnakameguro.xml");
            prop.storeToXML(stream, "libnakameguro properties.");
            stream.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new PropertyCreator();
    }
}