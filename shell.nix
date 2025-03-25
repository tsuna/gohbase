{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {

  packages = with pkgs; [
    hbase_2_4
    jdk8
    hadoop
    go_1_24
    procps
  ];

  shellHook = ''
    export JAVA_HOME="${pkgs.jdk8}"
    export HBASE_HOME="${pkgs.hbase}"
    export HBASE_LOG_DIR=logs

    # Debugging & Useful
    alias hbase-shell="$HBASE_HOME/bin/hbase shell"
    alias hbase-start="$HBASE_HOME/bin/start-hbase.sh"
    alias hbase-stop="$HBASE_HOME/bin/start-hbase.sh"
  '';

  exitHook = ''
    $HBASE_HOME/bin/stop-hbase.sh
  '';

}
