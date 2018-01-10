package es.amplia.cassandra.service;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.PagingStateException;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import es.amplia.cassandra.TestSpringBootCassandraApplication;
import es.amplia.cassandra.entity.*;
import es.amplia.model.AuditMessage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static es.amplia.cassandra.entity.AuditMessageEntity.Names.*;
import static es.amplia.cassandra.entity.Entity.Names.KEYSPACE;
import static es.amplia.cassandra.entity.Payload.Names.PAYLOAD_BY_ID_TABLE;
import static es.amplia.cassandra.service.ServiceTestUtils.*;
import static java.text.DateFormat.SHORT;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestSpringBootCassandraApplication.class)
public class NorthMessagesServiceIntegrationTest {

    @Autowired
    private NorthMessagesService northMessagesService;

    @Autowired
    private PayloadService payloadService;

    @Autowired
    private Session session;

    @Autowired
    private MappingManager mappingManager;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void cleanUp () throws ParseException {
        session.execute(truncate(KEYSPACE, NORTH_MESSAGES_BY_INTERVAL_TABLE));
        session.execute(truncate(KEYSPACE, NORTH_MESSAGES_BY_USER_INTERVAL_TABLE));
        session.execute(truncate(KEYSPACE, NORTH_MESSAGES_BY_USER_SUBJECT_INTERVAL_TABLE));
        session.execute(truncate(KEYSPACE, PAYLOAD_BY_ID_TABLE));
    }

    @Test
    public void given_a_repository_when_queried_by_specific_interval_then_verify_returned_messages_are_in_that_interval() throws ParseException {
        given_a_repository_with_a_collection_of_persisted_messages(30);
        Date from = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("01/01/2016 0:00:00");
        Date to = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("30/01/2016 0:00:00");
        List<Row> rows = session.execute(select()
                .all()
                .from(KEYSPACE, NORTH_MESSAGES_BY_INTERVAL_TABLE)
                .allowFiltering()
                .where(gte(OCCUR_TIME_FIELD, from))
                .and(lte(OCCUR_TIME_FIELD, to))).all();

        Page<NorthMessageByInterval> response = northMessagesService.getMessagesByInterval(from, to, null);
        assertThat(response.content, hasSize(rows.size()));
        for (NorthMessageByInterval message : response.content) {
            verify_abstractMessage_has_all_expected_values(session, message);
        }
    }

    @Test
    public void given_a_repository_with_more_rows_than_fetch_size_configured_when_queried_by_then_verify_returned_messages_are_paged() throws ParseException {
        int rowNum = 1000;
        int fetchSize = 100;
        given_a_repository_with_a_collection_of_persisted_messages(rowNum);
        Date from = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("01/01/2016 0:00:00");
        Date to = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("30/01/2016 0:00:00");

        String page = null;
        int pagesQueried = 0;
        List<NorthMessageByInterval> pagedRows = new ArrayList<>();

        Mapper<NorthMessageByInterval> mapper = mappingManager.mapper(NorthMessageByInterval.class);
        List<NorthMessageByInterval> fetchedRows = mapper.map(session.execute(select()
                .all()
                .from(KEYSPACE, NORTH_MESSAGES_BY_INTERVAL_TABLE)
                .allowFiltering()
                .where(gte(OCCUR_TIME_FIELD, from))
                .and(lte(OCCUR_TIME_FIELD, to)))).all();
        do {
            Page<NorthMessageByInterval> response = northMessagesService.getMessagesByInterval(from, to, page, fetchSize);
            page = response.pageContext;
            assertThat(response.content, hasSize(lessThanOrEqualTo(fetchSize)));
            pagedRows.addAll(response.content);
            for (NorthMessageByInterval message : response.content) {
                verify_abstractMessage_has_all_expected_values(session, message);
            }
            if (page != null)
                pagesQueried++;
        }
        while (page != null);
        assertThat(pagedRows, hasSize(fetchedRows.size()));
        assertThat(pagedRows, containsInAnyOrder(fetchedRows.toArray()));
        assertThat(fetchedRows.size()/fetchSize, is(pagesQueried));
    }

    @Test
    public void given_a_repository_with_more_rows_than_fetch_size_configured_when_queried_changing_page_size_then_verify_returned_messages_are_paged() throws ParseException {
        int numRowsInRepository = 1000;
        int fetchSize = 50;
        given_a_repository_with_a_collection_of_persisted_messages(numRowsInRepository);
        Date from = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("01/01/2016 0:00:00");
        Date to = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("30/01/2016 0:00:00");

        String page = null;
        int pagesQueried = 0;
        List<NorthMessageByInterval> pagedRows = new ArrayList<>();

        Mapper<NorthMessageByInterval> mapper = mappingManager.mapper(NorthMessageByInterval.class);
        List<NorthMessageByInterval> fetchedRows = mapper.map(session.execute(select()
                .all()
                .from(KEYSPACE, NORTH_MESSAGES_BY_INTERVAL_TABLE)
                .allowFiltering()
                .where(gte(OCCUR_TIME_FIELD, from))
                .and(lte(OCCUR_TIME_FIELD, to)))).all();
        do {
            Page<NorthMessageByInterval> response = northMessagesService.getMessagesByInterval(from, to, page, fetchSize);
            page = response.pageContext;
            assertThat(response.content, hasSize(lessThanOrEqualTo(fetchSize)));
            pagedRows.addAll(response.content);
            for (NorthMessageByInterval message : response.content) {
                verify_abstractMessage_has_all_expected_values(session, message);
            }
            pagesQueried++;
            fetchSize *= 2;
        }
        while (page != null);
        assertThat(pagedRows, containsInAnyOrder(fetchedRows.toArray()));
        assertThat(pagesQueried, is(5));
    }

    @Test
    public void given_a_repository_when_queried_by_specific_interval_without_messages_then_verify_returned_messages_is_empty() throws ParseException {
        given_a_repository_with_a_collection_of_persisted_messages(30);
        Date from = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("02/01/1975 0:00:00");
        Date to = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("07/01/1975 0:00:00");

        Page<NorthMessageByInterval> response = northMessagesService.getMessagesByInterval(from, to, null);
        assertThat(response.content, empty());
    }

    @Test
    public void given_a_repository_when_queried_by_specific_interval_and_user_then_verify_returned_messages_are_in_that_interval() throws ParseException {
        given_a_repository_with_a_collection_of_persisted_messages(30);
        String user = given_a_list_of_users().get(0);
        Date from = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("01/01/2016 0:00:00");
        Date to = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("30/01/2016 0:00:00");
        List<Row> rows = session.execute(select()
                .all()
                .from(KEYSPACE, NORTH_MESSAGES_BY_INTERVAL_TABLE)
                .allowFiltering()
                .where(gte(OCCUR_TIME_FIELD, from))
                .and(lte(OCCUR_TIME_FIELD, to))
                .and(eq(USER_FIELD, user))).all();

        Page<NorthMessageByUserInterval> response = northMessagesService.getMessagesByUserInterval(user, from, to, null);
        assertThat(response.content, hasSize(rows.size()));
        for (NorthMessageByUserInterval message : response.content) {
            assertThat(message.getUser(), is(user));
            verify_abstractMessage_has_all_expected_values(session, message);
        }
    }

    @Test
    public void given_a_repository_when_queried_by_specific_interval_user_and_subject_then_verify_returned_messages_are_in_that_interval() throws ParseException {
        given_a_repository_with_a_collection_of_persisted_messages(30);
        String user = given_a_list_of_users().get(0);
        String subject = given_a_list_of_subjects().get(0);
        Date from = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("01/01/2016 0:00:00");
        Date to = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("30/01/2016 0:00:00");
        List<Row> rows = session.execute(select()
                .all()
                .from(KEYSPACE, NORTH_MESSAGES_BY_INTERVAL_TABLE)
                .allowFiltering()
                .where(gte(OCCUR_TIME_FIELD, from))
                .and(lte(OCCUR_TIME_FIELD, to))
                .and(eq(USER_FIELD, user)).and(eq(SUBJECT_FIELD, subject))).all();

        Page<NorthMessageByUserSubjectInterval> response = northMessagesService.getMessagesByUserSubjectInterval(user, subject, from, to, null);
        assertThat(response.content, hasSize(rows.size()));
        for (NorthMessageByUserSubjectInterval message : response.content) {
            assertThat(message.getUser(), is(user));
            assertThat(message.getSubject(), is(subject));
            verify_abstractMessage_has_all_expected_values(session, message);
        }
    }

    @Test
    public void given_an_auditMessage_when_saved_then_its_persisted_into_all_related_tables() {
        Date occurTime = new Date();
        AuditMessage auditMessage = given_an_auditMessage(occurTime);
        when_saved(auditMessage);

        Mapper<NorthMessageByInterval> northMessageByIntervalMapper =
                mappingManager.mapper(NorthMessageByInterval.class);
        List<NorthMessageByInterval> northMessagesByInterval = northMessageByIntervalMapper.map(
                session.execute(select().all()
                        .from(KEYSPACE, NORTH_MESSAGES_BY_INTERVAL_TABLE).allowFiltering()
                        .where(eq(OCCUR_TIME_FIELD, occurTime)))).all();

        Mapper<NorthMessageByUserInterval> northMessageByUserIntervalMapper =
                mappingManager.mapper(NorthMessageByUserInterval.class);
        List<NorthMessageByUserInterval> northMessagesByUserInterval = northMessageByUserIntervalMapper.map(
                session.execute(select().all()
                        .from(KEYSPACE, NORTH_MESSAGES_BY_USER_INTERVAL_TABLE).allowFiltering()
                        .where(eq(OCCUR_TIME_FIELD, occurTime)))).all();

        Mapper<NorthMessageByUserSubjectInterval> northMessageByUserSubjectIntervalMapper =
                mappingManager.mapper(NorthMessageByUserSubjectInterval.class);
        List<NorthMessageByUserSubjectInterval> northMessagesByUserSubjectInterval =
                northMessageByUserSubjectIntervalMapper.map(
                        session.execute(select().all()
                                .from(KEYSPACE, NORTH_MESSAGES_BY_USER_SUBJECT_INTERVAL_TABLE).allowFiltering()
                                .where(eq(OCCUR_TIME_FIELD, occurTime)))).all();

        Payload payload = payloadService.getMessagePayload(northMessagesByInterval.get(0).getPayloadId());

        assertThat(northMessagesByInterval, hasSize(1));
        assertThat(northMessagesByUserInterval, hasSize(1));
        assertThat(northMessagesByUserSubjectInterval, hasSize(1));
        verify_payload_row_exists(session, northMessagesByInterval.get(0).getPayloadId());
        assertThat(northMessagesByInterval.get(0).getPayloadId(), allOf(
                equalTo(northMessagesByUserInterval.get(0).getPayloadId()),
                equalTo(northMessagesByUserSubjectInterval.get(0).getPayloadId())));
        assertThat(payload.getMsgPayload(), is(auditMessage.getMsgPayload()));
    }

    @Test
    public void given_a_repository_when_queried_with_wrong_paging_state_then_exception_thrown() throws ParseException {
        given_a_repository_with_a_collection_of_persisted_messages(30);
        expectedException.expect(PagingStateException.class);
        Date from = new Date();
        Date to = new Date();

        northMessagesService.getMessagesByInterval(from, to, "wrongPagingState");
    }

    @Test
    public void given_a_repository_when_queried_by_a_too_big_interval_then_exception_thrown() throws ParseException {
        given_a_repository_with_a_collection_of_persisted_messages(30);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("specified time range is too big, be more specific");
        Date from = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("02/01/2016 0:00:00");
        Date to = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("07/03/2016 0:00:00");

        northMessagesService.getMessagesByInterval(from, to, null);
    }

    @Test
    public void given_a_repository_when_queried_by_an_user_and_a_too_big_interval_then_exception_thrown() throws ParseException {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("specified time range is too big, be more specific");
        given_a_repository_with_a_collection_of_persisted_messages(30);
        Date from = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("02/01/2016 0:00:00");
        Date to = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("02/05/2016 0:00:00");

        northMessagesService.getMessagesByUserInterval("", from, to, null);
    }

    @Test
    public void given_a_repository_when_queried_by_an_user_subject_and_a_too_big_interval_then_exception_thrown() throws ParseException {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("specified time range is too big, be more specific");
        given_a_repository_with_a_collection_of_persisted_messages(30);
        Date from = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("02/01/2016 0:00:00");
        Date to = DateFormat.getDateTimeInstance(SHORT, SHORT).parse("02/01/2017 0:00:00");

        northMessagesService.getMessagesByUserSubjectInterval("", "", from, to, null);
    }

    private void given_a_repository_with_a_collection_of_persisted_messages(int num) throws ParseException {
        List<AuditMessage> auditMessages = given_a_collection_of_auditMessages(given_a_collection_of_dates(), num);
        for (AuditMessage auditMessage : auditMessages) {
            when_saved(auditMessage);
        }
    }

    private void when_saved(AuditMessage auditMessage) {
        northMessagesService.save(auditMessage);
    }

    private List<String> given_a_collection_of_dates() {
        return asList("01/01/2016T0:00:00.000", "02/01/2016T0:00:00.000", "03/01/2016T0:00:00.000",
                "04/01/2016T0:00:00.000", "05/01/2016T0:00:00.000", "06/01/2016T0:00:00.000",
                "07/01/2016T0:00:00.000", "08/01/2016T0:00:00.000", "09/01/2016T0:00:00.000",
                "10/01/2016T0:00:00.000", "11/01/2016T0:00:00.000", "12/01/2016T0:00:00.000",
                "13/01/2016T0:00:00.000", "14/01/2016T0:00:00.000", "15/01/2016T0:00:00.000",
                "16/01/2016T0:00:00.000", "17/01/2016T0:00:00.000", "18/01/2016T0:00:00.000",
                "19/01/2016T0:00:00.000", "20/01/2016T0:00:00.000", "21/01/2016T0:00:00.000",
                "22/01/2016T0:00:00.000", "23/01/2016T0:00:00.000", "24/01/2016T0:00:00.000",
                "25/01/2016T0:00:00.000", "26/01/2016T0:00:00.000", "27/01/2016T0:00:00.000",
                "28/01/2016T0:00:00.000", "29/01/2016T0:00:00.000", "30/01/2016T0:00:00.000"
        );
    }
}
