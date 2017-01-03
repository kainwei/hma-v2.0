/**
 *
 */
package hma.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author guoyezhi
 */
public class HttpClientHelper {
    public static final int ERROR_STATUS_CODE = -1;
    public static final int OK_STATUS_CODE = 200;

    /**
     * @param scheme
     * @param host
     * @param port
     * @param path
     * @param params
     * @param entityEncoding
     * @return
     * @throws java.net.URISyntaxException
     * @throws org.apache.http.client.ClientProtocolException
     *
     * @throws java.io.IOException
     */
    public static String getRequest(
            String scheme,
            String host,
            int port,
            String path,
            Map<String, String> params,
            String entityEncoding)
            throws URISyntaxException, ClientProtocolException, IOException {

        List<NameValuePair> paramList = new ArrayList<NameValuePair>();
        Iterator<Map.Entry<String, String>> iter = params.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            String param = entry.getKey();
            String value = entry.getValue();
            paramList.add(new BasicNameValuePair(param, value));
        }
        URI uri = URIUtils.createURI(
                scheme, host, port, path,
                URLEncodedUtils.format(paramList, "UTF-8"), null);
        HttpGet httpget = new HttpGet(uri);

        HttpClient httpclient = new DefaultHttpClient();
        HttpResponse response = httpclient.execute(httpget);

        String rspContent = null;
        HttpEntity rspEntity = response.getEntity();
        if (rspEntity != null) {
            rspContent = EntityUtils.toString(rspEntity);
        }
        EntityUtils.consume(rspEntity);

        return rspContent;
    }


    /**
     * @param url
     * @return
     */
    public static String postRequest(String url) {
        throw new UnsupportedOperationException();
    }

    /**
     * @param url
     * @param params
     * @param entityEncoding
     * @return
     * @throws java.io.UnsupportedEncodingException
     *
     * @throws org.apache.http.client.ClientProtocolException
     *
     * @throws java.io.IOException
     */
    public static String postRequest(
            String url,
            Map<String, String> params,
            String entityEncoding)
            throws UnsupportedEncodingException, ClientProtocolException, IOException {

        String rspContent = null;

        HttpClient httpclient = new DefaultHttpClient();
        HttpPost httppost = new HttpPost(url);
        List<NameValuePair> paramList = new ArrayList<NameValuePair>();
        Iterator<Map.Entry<String, String>> iter = params.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            String param = entry.getKey();
            String value = entry.getValue();
            paramList.add(new BasicNameValuePair(param, value));
        }
        UrlEncodedFormEntity reqEntity =
                new UrlEncodedFormEntity(paramList, entityEncoding);
        httppost.setEntity(reqEntity);
        HttpResponse response = httpclient.execute(httppost);
        HttpEntity rspEntity = response.getEntity();
        if (rspEntity != null) {
            rspContent = EntityUtils.toString(rspEntity);
        }
        EntityUtils.consume(rspEntity);

        return rspContent;
    }

    /**
     * @param url "http://caoliushequ.com?page=1"
     * @return int
     */
    public static int getStatusCode(String url) {
        URI uri = null;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return ERROR_STATUS_CODE;
        }
        HttpGet httpget = new HttpGet(uri);
        HttpClient httpclient = new DefaultHttpClient();
        HttpResponse response = null;
        try {
            response = httpclient.execute(httpget);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            LOG.warn("Failed to get http result from " + url);
            return ERROR_STATUS_CODE;
        }
        return response.getStatusLine().getStatusCode();

    }

    /**
     * @param url url to get
     * @return String
     */
    public static String get(String url) {
        URI uri = null;
        String rspContent = null;
        HttpResponse response = null;

        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return null;
        }
        HttpGet httpget = new HttpGet(uri);
        HttpClient httpclient = new DefaultHttpClient();
        try {
            response = httpclient.execute(httpget);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            LOG.warn("Failed to get http result from " + url);
            return null;
        }
        HttpEntity rspEntity = response.getEntity();
        if (rspEntity != null) {
            try {
                rspContent = EntityUtils.toString(rspEntity);
                EntityUtils.consume(rspEntity);
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                LOG.warn("Failed to get http result from " + url);
                return null;
            }
        }
        return rspContent;
    }


}
