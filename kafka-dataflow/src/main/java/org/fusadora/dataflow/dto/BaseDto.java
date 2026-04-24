package org.fusadora.dataflow.dto;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serial;
import java.io.Serializable;

/**
 * org.fusadora.dataflow.dto.BaseDto
 * All DTOs to extends this Base DTO class.Placeholder to add common properties
 * and methods.
 *
 * @author Parag Ghosh
 * @since 04/12/2025
 */
public abstract class BaseDto implements Serializable {

    protected static final ObjectMapper MAPPER = new ObjectMapper();
    @Serial
    private static final long serialVersionUID = 1L;

    protected BaseDto() {
    }

    protected BaseDto(BaseDto toCopy) {
    }
}
