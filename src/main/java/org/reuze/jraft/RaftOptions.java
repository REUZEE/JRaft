package org.reuze.jraft;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Reuze
 * @Date 30/12/2022
 */
@Getter
@Setter
public class RaftOptions {

    private int electionTimeoutMilliseconds = 5000;

    private int heartbeatPeriodMilliseconds = 500;

    private int raftConsensusThreadNum = 20;

}
