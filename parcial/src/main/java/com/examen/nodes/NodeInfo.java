package com.examen.nodes;

import java.io.PrintWriter;

public class NodeInfo {
  private String ip;
  private int port;
  private PrintWriter writer;

  public NodeInfo(String ip, int port, PrintWriter writer) {
    this.ip = ip;
    this.port = port;
    this.writer = writer;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public PrintWriter getWriter() {
    return writer;
  }

  public void setWriter(PrintWriter writer) {
    this.writer = writer;
  }
}
