package macros.htmlchartsreporter;

import java.util.HashMap;
import java.util.Map;

import org.benchkit.RecordingData;
import org.benchkit.Reporter;

public class HtmlChartsReporter implements Reporter {
  private final DataInterpretor interpretor;
  private final Map<String, Chart> charts;
  private Object[] arguments;

  public HtmlChartsReporter(DataInterpretor interpretor) {
    this.interpretor = interpretor;
    charts = new HashMap<>();
  }

  @Override
  public void setArguments(Object[] arguments) {
    this.arguments = arguments;
  }

  @Override
  public void report(RecordingData recordingData) {
    String chartName = interpretor.getChartName(arguments);
    String seriesName = interpretor.getSeriesName(arguments);
    String xaxisName = interpretor.getXaxisName(arguments);
    String yaxisName = interpretor.getYaxisName(arguments);
    Comparable<Object> xvalue = interpretor.getXvalue(arguments);
    System.out.printf(
        "chart=%s, series=%s, x-axis=%s, y-axis=%s, x=%s\n",
        chartName, seriesName, xaxisName, yaxisName, xvalue);
    
    
    
    Chart chart = getOrCreateChart();
    DataPoint dataPoint = createDataPoint(recordingData);
    chart.addDataPoint(dataPoint);
  }

  private DataPoint createDataPoint(RecordingData recordingData) {
    String seriesName = interpretor.getSeriesName(arguments);
    Comparable<Object> xvalue = interpretor.getXvalue(arguments);
    
    // trouble: want to show both through-put and latency in the same chart...
    // maybe each chart is in two parts?
    
    DataPoint dataPoint = new DataPoint(seriesName, xvalue);
    return dataPoint;
  }

  private Chart getOrCreateChart() {
    String chartName = interpretor.getChartName(arguments);
    Chart chart = charts.get(chartName);
    if (chart == null) {
      String xaxisName = interpretor.getXaxisName(arguments);
      String yaxisName = interpretor.getYaxisName(arguments);
      chart = new Chart(chartName, xaxisName, yaxisName);
      charts.put(chartName, chart);
    }
    return chart;
  }

  @Override
  public void clear() {
    arguments = null;
  }
}
