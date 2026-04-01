package org.fusadora.common;

/**
 * org.fusadora.common.Constants
 * Constants used across the Fusadora project.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public class Constants {

    //CheckPointing : Firestore constants
    public static final String CHECKPOINT_DOCUMENT_TOPIC = "topic";
    public static final String CHECKPOINT_DOCUMENT_PARTITION = "partition";
    public static final String CHECKPOINT_DOCUMENT_NEXT_OFFSET_TO_READ = "nextOffsetToRead";
    public static final String CHECKPOINT_DOCUMENT_LAST_ACKED_OFFSET = "lastAckedOffset";
    public static final String CHECKPOINT_DOCUMENT_UPDATED_AT = "updatedAt";
    public static final String CHECKPOINT_DOCUMENT_UPDATED_BY = "updatedByJobId";

    private Constants() {
        throw new IllegalStateException("Utility class");
    }
}
