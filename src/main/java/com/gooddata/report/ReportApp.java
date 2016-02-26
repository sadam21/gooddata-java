/*
 * Copyright (C) 2007-2016, GoodData(R) Corporation. All rights reserved.
 *
 */

package com.gooddata.report;

import com.gooddata.GoodData;
import com.gooddata.md.MetadataService;
import com.gooddata.md.report.Report;

public class ReportApp {

    public static void main(String[] args) {
        final GoodData gd = new GoodData("zebroids.intgdc.com", "adam.stulpa+zendesk@gooddata.com", "<>");
        final MetadataService md = gd.getMetadataService();

        final Report report = md.getObjByUri("/gdc/md/b27pwwvlz5fnvdpiy4w3q257t1yvbw18/obj/42447", Report.class);

        System.out.println("Report def:");

        System.out.println("Report:");
        gd.getReportService().exportReportRawCsvHost(report, System.out);
    }
}
