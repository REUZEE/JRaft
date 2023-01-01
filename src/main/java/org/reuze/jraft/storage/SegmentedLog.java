package org.reuze.jraft.storage;

import lombok.Getter;
import lombok.Setter;
import org.reuze.jraft.proto.RaftProto;

/**
 * @author Reuze
 * @Date 30/12/2022
 */
@Setter
@Getter
public class SegmentedLog {

    private RaftProto.LogMetaData metaData = RaftProto.LogMetaData.newBuilder().build();

    public long getLastLogIndex() {
        // todo
        return 0L;
    }

    public long getFirstLogIndex() {
        // todo
        return 0L;
        // return metaData.getFirstLogIndex();
    }

    public long getEntryTerm(long index) {
        // todo
        return 0L;
    }

    public void updateMetaData(Long currentTerm, Integer voteFor, Long firstLogIndex, Long commitIndex) {
        // todo
    }

}
