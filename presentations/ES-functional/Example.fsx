#r "nuget: FSToolkit.ErrorHandling.TaskResult"
#r "nuget: FSharp.Json"
#r "nuget: Microsoft.Azure.Cosmos"

open System
open System.Text.RegularExpressions
open FsToolkit.ErrorHandling
open FSharp.Json

// Primitives

type FirstName = FirstName of string
type LastName = LastName of string
type MiddleName = MiddleName of string

type EmailAddress = private EmailAddress of string
module EmailAddress =
  let create email = 
    if Regex.IsMatch(email, ".*?@(.*)")
      then email |> EmailAddress |> Ok
      else Error "Incorrect email format"
  let value (EmailAddress e) = e

type Name = 
  { FirstName: FirstName
    MiddleName: MiddleName option
    LastName: LastName }

type ContactDetails = { Name: Name; Email: EmailAddress }

// Events

type VaccinationEvent =
  | ContactRegistered of ContactDetails
  | AppointmentCreated of VaccinationAppointment
  | AppointmentCanceled of AppointmentId
  | VaccineAdministered of AppointmentId
  | ObservationComplete of AppointmentId * ObservationStatus
  | SurveySubmitted of AppointmentId * SurveyResult
and VaccinationAppointment =
  { Id: AppointmentId
    Vaccine: VaccineType
    Hub: VaccinationHub
    Date: DateTime }
and AppointmentId = string
and VaccineType = Pfizer | Moderna | AstraZeneca
and VaccinationHub = exn
and ObservationStatus = | NoAdverseReaction | AdverseReaction of ReactionKind
and ReactionKind = exn
and SurveyResult = exn

type EventMetadata = 
  { Id: EventId; Timestamp: DateTime; StreamId: StreamId; Version: int }
and EventId = EventId of Guid
and StreamId = StreamId of string

type Event<'D, 'M> = { Id: Guid; Data: 'D; Metadata: 'M }

// Commands

type VaccinationCommand =
  | RegisterContact of ContactDetails
  | CreateAppointment of VaccinationAppointment
  | CancelAppointment of AppointmentId
  | AdministerVaccine of AppointmentId 
                      * ObservationStatus option
  | SubmitSurvey of AppointmentId * SurveyResult

type Command<'D, 'M> = { Id: Guid; Data: 'D; Metadata: 'M }

let toEvents c = function
  | RegisterContact cd -> [ ContactRegistered cd ]
  | CreateAppointment a -> [ AppointmentCreated a ]
  | CancelAppointment appId -> [ AppointmentCanceled appId ]
  | AdministerVaccine (appId, None) -> 
    [ VaccineAdministered appId ]
  | AdministerVaccine (appId, Some status) -> 
    [ VaccineAdministered appId
      ObservationComplete (appId, status) ]
  | SubmitSurvey (appId, s) -> [ SurveySubmitted (appId, s) ]

// The states of the aggregate

type VaccineRecipient = 
  { Id: Guid
    ContactDetails: ContactDetails
    RegistrationDate: DateTime
    State: VaccineRecipientState }
and VaccineRecipientState =
  | Registered
  | Booked of VaccinationAppointment nlist
  | InProcess of VaccinationInProcess
  | FullyVaccinated of VaccinationResult nlist
and VaccinationInProcess =
  { Administered: VaccinationResult nlist
    Booked: VaccinationAppointment nlist }
and VaccinationResult = VaccinationAppointment
                      * ObservationStatus option
                      * SurveyResult option
and 'T nlist = NonEmptyList<'T>
and NonEmptyList<'T> = 'T list

// Domain logic

type Folder<'Aggregate, 'Event> =
  'Aggregate -> 'Event list -> 'Aggregate

type Handler<'Aggregate, 'Command, 'Event> = 
  'Aggregate -> 'Command -> 'Aggregate * 'Event list


let create newId timestamp event =
  match event with 
  | ContactRegistered c -> 
    { Id = newId; ContactDetails = c; 
      RegistrationDate = timestamp; State = Registered } |> Ok
  | _ -> Error "Aggregate doesn't exist"

let update aggregate event =
  match aggregate.State, event with
  | Registered, AppointmentCreated apt -> 
    { aggregate with State = Booked [ apt ] } |> Ok
  | Booked list, AppointmentCreated apt ->
    { aggregate with State = Booked (list @ [apt] ) } |> Ok
  | _, _ -> "Exercise left to the reader" |> Error

// DTOs

type NameDto =
  { FirstName: string
    MiddleName: string // can be null :(
    LastName: string }

module NameDto = 

  let toDomain (name: NameDto) = 
    ({ FirstName = name.FirstName |> FirstName
       MiddleName = name.MiddleName 
                    |> Option.ofObj |> Option.map MiddleName
       LastName = name.LastName |> LastName }: Name) |> Ok

  let ofDomain (name: Name) = 
    let (FirstName firstName) = name.FirstName
    let ofMiddleName (MiddleName middleName) = middleName
    let middleName = name.MiddleName 
                     |> Option.map ofMiddleName
                     |> Option.toObj
    let (LastName lastName) = name.LastName
    let dto = { FirstName = firstName
                MiddleName = middleName
                LastName = lastName }
    dto

type ContactDetailsDto = { Name: NameDto; Email: string }

module ContactDetailsDto =

  let toDomainTheBoringWay contact =
    let nameResult = contact.Name |> NameDto.toDomain
    let emailResult = contact.Email |> EmailAddress.create
    match (nameResult, emailResult) with
    | Ok name, Ok email -> 
      ({ Name = name; Email = email }: ContactDetails) |> Ok
    | Error e, _ -> e |> Error
    | _, Error e -> e |> Error

  let toDomain contact = result {
    let! name = contact.Name |> NameDto.toDomain
    let! email = contact.Email |> EmailAddress.create
    return ({ Name = name; Email = email }: ContactDetails)
  }

// Serialization

let tryParse<'Data> s =
  try
    Json.deserialize<'Data> s |> Ok
  with
    | x -> sprintf "Cannot deserialize: %s" x.Message |> Error

let record: Name = 
  { FirstName = FirstName "John"
    MiddleName = None
    LastName = LastName "Smith" }
record |> Json.serialize |> printfn "%s"


let readContacts contactsString =
  contactsString 
  |> tryParse<ContactDetailsDto>
  |> Result.bind ContactDetailsDto.toDomain

// Persistence

module Db = 
  type PartitionKey = PartitionKey of string
  type Id = Id of string
  type ETag = ETag of string

type EntityType = Event | AggregateRoot

type PersistentEntity<'payload> =
  { [<JsonField("partitionKey")>]
    PartitionKey: Db.PartitionKey
      
    [<JsonField("id")>]
    Id: Db.Id
      
    [<JsonField("_etag")>]
    ETag: Db.ETag option
      
    Type: EntityType
      
    Payload: 'payload }

module ProfileKeyStrategy =
  let toPartitionKey streamId = 
    sprintf "VaccineRecipient-%s" streamId |> Db.PartitionKey

module VaccinationPersistence = 
  type Event = PersistentEntity<VaccinationEvent>
  type Aggregate = PersistentEntity<VaccineRecipient>

// CosmosDB Batch API

module CosmosDb = 
  open Microsoft.Azure.Cosmos
  open System.Text
  open System.IO

  let createBatch (Db.PartitionKey key) (container: Container) =
    container.CreateTransactionalBatch(PartitionKey(key))

  let createAsString (payload: string) 
                     (batch: TransactionalBatch) =
    new MemoryStream(Encoding.UTF8.GetBytes(payload))
    |> batch.CreateItemStream

  let replaceAsString (Db.Id id) (payload: string) 
                      (Db.ETag etag) 
                      (batch: TransactionalBatch) =
    let stream = new MemoryStream(Encoding.UTF8.GetBytes(payload))
    let options = TransactionalBatchItemRequestOptions()
    options.IfMatchEtag <- etag
    batch.ReplaceItemStream(id, stream, options)

  let executeBatch (batch: TransactionalBatch) = taskResult {
    let! response = batch.ExecuteAsync()
    if (response.IsSuccessStatusCode) then
      return! Ok ()
    else
      return! Error (response.StatusCode, response.ErrorMessage)
  }

module Example =
  type ErrorMessage = string
  type DomainEvent = Event<VaccinationEvent, EventMetadata> 
  type HandlerResult =
    | NoChange
    | InvalidCommand of ErrorMessage
    | Conflict of ErrorMessage
    | CreateAggregate of VaccineRecipient * DomainEvent list
    | UpdateAggregate of VaccineRecipient * DomainEvent list