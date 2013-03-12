package macros.htmlchartsreporter;

public interface DataInterpretor {
  Comparable<Object> getXvalue(Object[] args);
  // the Y value is derived from the benchmarking data
  String getXaxisName(Object[] args);
  String getYaxisName(Object[] args);
  String getChartName(Object[] args);
  String getSeriesName(Object[] args);
}
