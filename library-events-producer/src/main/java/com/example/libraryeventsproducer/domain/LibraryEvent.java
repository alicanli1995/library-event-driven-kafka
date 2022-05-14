package com.example.libraryeventsproducer.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class LibraryEvent {

    private Integer libraryEventId;

    private LibraryEventType libraryEventType;

    @NotNull
    @Valid
    private Book book;


}
