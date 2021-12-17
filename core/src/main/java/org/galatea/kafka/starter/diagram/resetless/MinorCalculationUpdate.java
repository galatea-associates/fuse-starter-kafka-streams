package org.galatea.kafka.starter.diagram.resetless;


import org.galatea.kafka.starter.diagram.KvPair;
import org.galatea.kafka.starter.diagram.Participant;
import org.galatea.kafka.starter.diagram.State;
import org.galatea.kafka.starter.diagram.UmlBuilder;

public class MinorCalculationUpdate {

  public static void main(String[] args) {

    String oldVersion = "v1";
    String newVersion = "v2";

    KvPair versionPair = new KvPair("Version", oldVersion);
    KvPair pair = new KvPair("key1", "value1");
    KvPair ctxPair = new KvPair("CtxKey1", pair);
    KvPair outputPairPreRecalc = ctxPair.withValue("CalcValue1");
    KvPair recalcStarted = new KvPair("RecalcStarted", newVersion);
    KvPair outputPairPostRecalc = ctxPair.withValue("CalcValue2");

    UmlBuilder builder = new UmlBuilder();
    Participant it = builder.participant("InputTopic");
    Participant ip = builder.participant("InputProcessor");
    Participant repart = builder.participant("Repartition");
    Participant dp = builder.participant("DataProcessor");
    Participant os = builder.participant("OutputStream");

    State ipState = builder.state(ip);
    State dpState = builder.state(dp);

    // initial values
    ipState.put(pair);
    ipState.logState();

    dpState.put(State.SYSTEM_STORE, versionPair);
    dpState.put(State.INPUT_STORE, ctxPair);
    dpState.put(State.INTERMEDIATE_STORE, outputPairPreRecalc);
    dpState.put(State.OUTPUT_STORE, prefixKey(outputPairPreRecalc, oldVersion));
    dpState.logState(true);

    dp.note("Recalculation triggered");
    dp.note("Wipe intermediate state");
    dpState.remove("intermediate", outputPairPreRecalc);
    dpState.put(State.SYSTEM_STORE, recalcStarted);
    dpState.logState();

    dp.chain("Refresh request", repart, ip);

    ip.note("send all records");
    ip.chain(ctxPair, repart, dp);

    dp.note("Process - create OutputValue2");
    dpState.put(State.INTERMEDIATE_STORE, outputPairPostRecalc);
    dpState.put(State.OUTPUT_STORE, prefixKey(outputPairPostRecalc, newVersion));
    dpState.logState();

    dp.note("Suppress output until recalc complete");

    ip.chain("Refresh complete", repart, dp);

    dp.note("  // Check for keys that were previously output but not after recalc** - tombstone\n" +
        "  iterate outputStore v1\n" +
        "  if (!outputStore.contains([v2, CtxKey1])) {\n" +
        "    forward(CtxKey1, null)\n" +
        "    outputStore.remove([v1, CtxKey1])\n" +
        "  }\n" +
        "\n" +
        "  // Check for keys that were previously output but updated after recalc - update\n" +
        "  iterate outputStore v2\n" +
        "  if (outputStore.get([v1, CtxKey1]) != OutputValue1) {\n" +
        "    forward(CtxKey1, OutputValue1)\n" +
        "  }\n" +
        "  outputStore.remove([v1, CtxKey1])\n" +
        "\n" +
        "  update Version with RecalcVersion");

    dp.note("Recalc complete, output differences");
    dp.to(os, outputPairPostRecalc);

    dp.note("Update state");
    dpState.put(State.SYSTEM_STORE, versionPair.withValue(newVersion));
    dpState.remove(State.SYSTEM_STORE, recalcStarted);
    dpState.remove(State.OUTPUT_STORE, prefixKey(outputPairPostRecalc, oldVersion));
    dpState.logState();

    System.out.println(builder.build());
    /*

note over dp: CtxKey1 had same value between versions, no output

note over dp
  State:
  (system) Version: v2
  (input) {CtxKey1, [key1, value1]}
  (intermediate) {CtxKey1, intermediateState}
  (output) {[v2, CtxKey1], OutputValue1}
end note

left footer <font color=black>** Don't just look for current version, look for all versions less than RecalculateVersion</font>
@enduml
     */
  }

  private static KvPair prefixKey(KvPair pair, String prefix) {
    return new KvPair(new KvPair(prefix, pair.getKey()), pair.getValue());
  }
}
