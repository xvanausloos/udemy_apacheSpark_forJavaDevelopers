package com.ldi;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.Buffer;
import java.util.HashSet;
import java.util.Set;

/**
 * A wrapper for an input file containing a list of what we think are "boring" words.
 * * The list was generated by running a word count across all of VirtualPairProgrammer's subtitle files.
 * * Words that appear in every single course must (we think) be "boring" - ie they don't have a relevance
 * to just one specific course.
 * * This list of words is "small data" - ie it can be safely loaded into the driver's JVM - no need to
 * distribute this data.
 */
public class Util {
    private static Set<String> borings = new HashSet<String>();
    static Logger logger = Logger.getLogger(Util.class.getName());

    static {
        InputStream is = Util.class.getResourceAsStream("/subtitles/boringwords.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        br.lines().forEach(borings::add);
    }

    /**
     * Returns true if we think the word is "boring" - ie it doesn't seem to be a keyword
     * for a training course.
     */
    public static boolean isBoring(String word)
    {
        logger.info("is boring " + word);
        return borings.contains(word);
    }

    /**
     * Convenience method for more readable client code
     */
    public static boolean isNotBoring(String word)
    {
        return !isBoring(word);
    }
}