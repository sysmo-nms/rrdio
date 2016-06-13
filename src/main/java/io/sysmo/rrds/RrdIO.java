/* Copyright (c)2015-2016 Sebastien Serre <ssbx@sysmo.io>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.sysmo.rrds;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;

import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDbPool;
import org.rrd4j.core.Sample;
import org.rrd4j.graph.RrdGraph;
import org.rrd4j.graph.RrdGraphConstants;
import org.rrd4j.graph.RrdGraphDef;
import org.rrd4j.ConsolFun;

import java.awt.Color;

import javax.imageio.ImageIO;

import javax.json.Json;
import javax.json.JsonValue;
import javax.json.JsonArray;
import javax.json.JsonReaderFactory;
import javax.json.JsonObject;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Sebastien Serre on 09/07/15
 */

public class RrdIO
{
    private static ThreadPoolExecutor threadPool;
    private static BufferedOutputStream output;
    static RrdDbPool rrdDbPool;
    static Logger logger = Logger.getLogger(RrdIO.class.getName());
    static boolean active = true;

    public static void main(final String[] args) throws Exception
    {
        RrdIO.logger.setLevel(Level.WARNING);
        /*
         * Handle kill
         */
        Runtime.getRuntime().addShutdownHook(
                new Thread() {
                    @Override
                    public void run() {
                        RrdIO.active = false;
                    }
                }
        );

        /*
         * Disable disk caching for ImageIO (fastest)
         */
        ImageIO.setUseCache(false);

        /*
         * init thread pool and rrdDbPool
         */
        RrdIO.rrdDbPool = RrdDbPool.getInstance();
        RrdIO.threadPool = new ThreadPoolExecutor(
                12, //thread Core Pool Size
                20, //thread Max Pool Size
                10,
                TimeUnit.MINUTES,
                new ArrayBlockingQueue<Runnable>(3000), // 3000 = queue capacity
                new RrdReject()
        );

        /*
         * initialize output buffer.
         */
        int buffSize = 1000000;
        RrdIO.output = new BufferedOutputStream(System.out, buffSize);

        /*
         * Begin loop listen System.in
         */
        try {
            InputStream input = System.in;
            byte[] header = new byte[4];
            byte[] buffer = new byte[65535];
            int size, read, status;
            JsonReaderFactory readerFactory = Json.createReaderFactory(null);
            JsonObject jsonObject;

            while (RrdIO.active) {
                /*
                 * Get the first byte (as int 0 to 255 or -1 if EOF)
                 */
                status = input.read();
                if (status == -1) throw new IOException("STDIN broken");

                /*
                 * Complete the header[4]
                 */
                header[0] = (byte)status;
                header[1] = (byte)input.read();
                header[2] = (byte)input.read();
                header[3] = (byte)input.read();

                /*
                 * Compute the size of the message
                 */
                size = ByteBuffer.wrap(header, 0, 4).getInt();

                /*
                 * Now we can read the message
                 */
                read = 0;
                while (read != size) {
                    read += input.read(buffer, read, size - read);
                }

                /*
                 * Create a json object from the buffer
                 */
                jsonObject = readerFactory
                        .createReader(new ByteArrayInputStream(buffer,0,size))
                        .readObject();

                /*
                 * Start Runnable Job
                 */
                RrdIO.threadPool.execute(new RrdIOJob(jsonObject));
            }

        } catch (Exception e) {
            RrdIO.logger.log(Level.SEVERE, e.toString());
        }

        System.exit(1);
    }

    public static synchronized void rrdReply(final JsonObject object)
    {
        try {
            ByteBuffer b = ByteBuffer.allocate(4);
            byte[] reply = object.toString().getBytes("US-ASCII");
            b.putInt(reply.length);

            RrdIO.output.write(b.array(), 0, 4);
            RrdIO.output.write(reply,     0, reply.length);
            RrdIO.output.flush();
        }
        catch (Exception e)
        {
            RrdIO.logger.log(Level.SEVERE, e.toString());
        }
    }

    public static Color decodeRGBA(final String hexString)
    {
        return new Color(
                Integer.valueOf(hexString.substring(1,3), 16),
                Integer.valueOf(hexString.substring(3,5), 16),
                Integer.valueOf(hexString.substring(5,7), 16),
                Integer.valueOf(hexString.substring(7,9), 16)
        );
    }


    static class RrdReject implements RejectedExecutionHandler
    {
        public void rejectedExecution(
                final Runnable r, final ThreadPoolExecutor executor)
        {
            RrdIOJob failRunner = (RrdIOJob) r;
            int queryId = failRunner.getQueryId();
            JsonObject reply = Json.createObjectBuilder()
                    .add("queryId", queryId)
                    .add("reply", "Error thread queue full!")
                    .build();
            RrdIO.rrdReply(reply);
        }
    }

    static class RrdIOGraphDef extends RrdGraphDef
    {
        private static Color BACK_C;
        private static Color CANVAS_C;
        private static Color SHADEA_C;
        private static Color SHADEB_C;
        private static Color GRID_C;
        private static Color MGRID_C;
        private static Color FONT_C;
        private static Color FRAME_C;
        private static Color ARROW_C;
        private static Color XAXIS_C;

        public RrdIOGraphDef()
        {
            super(); // RrdGraphDef()
            this.setColor(RrdGraphConstants.COLOR_BACK,   BACK_C);
            this.setColor(RrdGraphConstants.COLOR_CANVAS, CANVAS_C);
            this.setColor(RrdGraphConstants.COLOR_SHADEA, SHADEA_C);
            this.setColor(RrdGraphConstants.COLOR_SHADEB, SHADEB_C);
            this.setColor(RrdGraphConstants.COLOR_GRID,   GRID_C);
            this.setColor(RrdGraphConstants.COLOR_MGRID,  MGRID_C);
            this.setColor(RrdGraphConstants.COLOR_FONT,   FONT_C);
            this.setColor(RrdGraphConstants.COLOR_FRAME,  FRAME_C);
            this.setColor(RrdGraphConstants.COLOR_ARROW,  ARROW_C);
            this.setColor(RrdGraphConstants.COLOR_XAXIS,  XAXIS_C);
            this.setImageFormat("png");
            this.setShowSignature(false);
            this.setAntiAliasing(true);
            this.setTextAntiAliasing(true);
            this.setImageQuality((float) 1.0);
            this.setLazy(false);
            //this.setFontSet(true);
        }

        public static void setDefaultColors(final JsonObject colorCfg)
        {
            JsonObject col;
            col = colorCfg.getJsonObject("BACK");
            RrdIOGraphDef.BACK_C =
                    new Color(col.getInt("red"), col.getInt("green"),
                            col.getInt("blue"), col.getInt("alpha"));

            col = colorCfg.getJsonObject("CANVAS");
            RrdIOGraphDef.CANVAS_C =
                    new Color(col.getInt("red"), col.getInt("green"),
                            col.getInt("blue"), col.getInt("alpha"));

            col = colorCfg.getJsonObject("SHADEA");
            RrdIOGraphDef.SHADEA_C =
                    new Color(col.getInt("red"), col.getInt("green"),
                            col.getInt("blue"), col.getInt("alpha"));

            col = colorCfg.getJsonObject("SHADEB");
            RrdIOGraphDef.SHADEB_C =
                    new Color(col.getInt("red"), col.getInt("green"),
                            col.getInt("blue"), col.getInt("alpha"));

            col = colorCfg.getJsonObject("GRID");
            RrdIOGraphDef.GRID_C =
                    new Color(col.getInt("red"), col.getInt("green"),
                            col.getInt("blue"), col.getInt("alpha"));

            col = colorCfg.getJsonObject("MGRID");
            RrdIOGraphDef.MGRID_C =
                    new Color(col.getInt("red"), col.getInt("green"),
                            col.getInt("blue"), col.getInt("alpha"));

            col = colorCfg.getJsonObject("FONT");
            RrdIOGraphDef.FONT_C =
                    new Color(col.getInt("red"), col.getInt("green"),
                            col.getInt("blue"), col.getInt("alpha"));

            col = colorCfg.getJsonObject("FRAME");
            RrdIOGraphDef.FRAME_C =
                    new Color(col.getInt("red"), col.getInt("green"),
                            col.getInt("blue"), col.getInt("alpha"));

            col = colorCfg.getJsonObject("ARROW");
            RrdIOGraphDef.ARROW_C =
                    new Color(col.getInt("red"), col.getInt("green"),
                            col.getInt("blue"), col.getInt("alpha"));

            col = colorCfg.getJsonObject("XAXIS");
            RrdIOGraphDef.XAXIS_C =
                    new Color(col.getInt("red"), col.getInt("green"),
                            col.getInt("blue"), col.getInt("alpha"));
        }
    }


    static class RrdIOJob implements Runnable
    {
        private JsonObject job;

        public RrdIOJob(final JsonObject job) { this.job = job; }

        public int getQueryId() {
            return this.job.getInt("queryId");
        }

        @Override
        public void run()
        {
            RrdIO.logger.log(Level.INFO, this.job.toString());

            String cmdType = this.job.getString("type");
            switch (cmdType)
            {
                case "graph":
                    this.handleGraph();
                    break;
                case "update":
                    this.handleUpdate();
                    break;
                case "color_config":
                    this.handleConfig();
                    break;
                default:
                    RrdIO.logger.log(Level.SEVERE, "Unknown command: " + cmdType);
            }
        }

        private void handleConfig()
        {
            RrdIOGraphDef.setDefaultColors(this.job);
        }

        private void handleUpdate()
        {
        /*
         * get arguments
         */
            String rrdFile    = this.job.getString("file");
            long timestamp    = (long)this.job.getInt("timestamp");
            JsonObject update = this.job.getJsonObject("updates");
            String opaque     = this.job.getString("opaque");
            int queryId       = this.job.getInt("queryId");

            String replyStatus;

        /*
         * open and write rrd db
         */
            RrdDb rrdDb = null;
            try {
                rrdDb = RrdIO.rrdDbPool.requestRrdDb(rrdFile);
                Sample sample = rrdDb.createSample();
                sample.setTime(timestamp);

                Set<String> updateKeys = update.keySet();
                for (String key : updateKeys)
                    sample.setValue(key, (long) update.getInt(key));

                sample.update();
                replyStatus = "success";

            } catch (Exception e) {
                RrdIO.logger.log(Level.SEVERE, e.toString());
                replyStatus = "failure";
            } finally {
                if (rrdDb != null) {
                    try {
                        RrdIO.rrdDbPool.release(rrdDb);
                    } catch (IOException inner) {
                        // ignore
                    }
                }
            }

        /*
         * Build and send reply
         */
            JsonObject reply = Json.createObjectBuilder()
                    .add("reply",   replyStatus)
                    .add("opaque",  opaque)
                    .add("queryId", queryId)
                    .build();
            RrdIO.rrdReply(reply);
        }

        private void handleGraph()
        {
        /*
         * Get logical arguments
         */
            String opaque = this.job.getString("opaque");
            int queryId = this.job.getInt("queryId");

        /*
         * Get graph arguments
         */
            String rrdFile = this.job.getString("rrdFile");
            String pngFile = this.job.getString("pngFile");

            String title  = this.job.getString("title");
            String vlabel = this.job.getString("verticalLabel");

            int spanBegin = this.job.getInt("spanBegin");
            int spanEnd   = this.job.getInt("spanEnd");

            int width  = this.job.getInt("width");
            int height = this.job.getInt("height");

            String minVal = this.job.getString("minimum");
            String maxVal = this.job.getString("maximum");

            String rigid   = this.job.getString("rigid");
            String base    = this.job.getString("base");
            String unit    = this.job.getString("unit");
            String unitExp = this.job.getString("unitExponent");

        /*
         * Generate the graph definition.
         */
            RrdIOGraphDef graphDef = new RrdIOGraphDef();
            graphDef.setTimeSpan(spanBegin, spanEnd);
            graphDef.setTitle(title);
            graphDef.setVerticalLabel(vlabel);
            graphDef.setFilename(pngFile);
            graphDef.setBase(Double.parseDouble(base));
            graphDef.setWidth(width);
            graphDef.setHeight(height);

            if (rigid.equals("true")) {
                graphDef.setRigid(true);
            }

            if (!unit.equals("")) {
                graphDef.setUnit(unit);
            }

            if (!unitExp.equals("")) {
                try {
                    int unitExpInt = Integer.parseInt(unitExp);
                    graphDef.setUnitsExponent(unitExpInt);
                } catch (Exception e) {
                    RrdIO.logger.log(Level.INFO,
                            "bad unit exp: " + unitExp + " " + e.toString());
                }
            }

            if (!minVal.equals("")) {
                try {
                    double minValDouble = Double.parseDouble(minVal);
                    graphDef.setMinValue(minValDouble);
                } catch (Exception e) {
                    RrdIO.logger.log(Level.INFO,
                            "min val not a double: " + e.toString());
                }
            }

            if (!maxVal.equals("")) {
                try {
                    double maxValDouble = Double.parseDouble(maxVal);
                    graphDef.setMaxValue(maxValDouble);
                } catch (Exception e) {
                    RrdIO.logger.log(Level.INFO,
                            "max val not a double: " + e.toString());
                }
            }

        /*
         * Get DS Draw list and iterate.
         */
            JsonArray dataSources = this.job.getJsonArray("draws");
            for(JsonValue ds : dataSources)
            {
                JsonObject obj = (JsonObject)ds;
                String dsName   = obj.getString("dataSource");
                String dsColor  = obj.getString("color");
                String dsLegend = obj.getString("legend");
                String dsType   = obj.getString("type");
                String calc     = obj.getString("calculation");

                Color color = RrdIO.decodeRGBA(dsColor);
                ConsolFun dsCons = ConsolFun.valueOf(
                        obj.getString("consolidation"));

                graphDef.datasource(dsName, rrdFile, dsName, dsCons);

                String drawName;
                if (calc.equals("")) {
                    drawName = dsName;
                } else {
                    drawName = "calculation-" + dsName;
                    graphDef.datasource(drawName, calc);
                }

                boolean forgetLegend = dsLegend.equals("none");

                switch (dsType)
                {
                    case "area":
                        if (forgetLegend) {
                            graphDef.area(drawName, color);
                        } else {
                            graphDef.area(drawName, color, dsLegend);
                        }
                        break;
                    case "stack":
                        if (forgetLegend) {
                            graphDef.stack(drawName, color);
                        } else {
                            graphDef.stack(drawName, color, dsLegend);
                        }
                        break;
                    default:
                        if (forgetLegend) {
                            graphDef.line(drawName, color);
                        } else {
                            graphDef.line(drawName, color, dsLegend);

                        }
                }
            }

        /*
         * Graph:
         */
            String replyStatus;
            try {
                new RrdGraph(graphDef);
                replyStatus = "success";

            } catch (IOException e) {
                RrdIO.logger.log(Level.WARNING,
                        "fail to generate graph: " + e.toString());
                replyStatus = "failure";
            }

        /*
         * Build and send reply
         */
            RrdIO.logger.log(Level.FINE, dataSources.toString());
            JsonObject reply = Json.createObjectBuilder()
                    .add("reply",   replyStatus)
                    .add("opaque",  opaque)
                    .add("queryId", queryId)
                    .build();
            RrdIO.rrdReply(reply);
        }
    }
}
