package tmuxlock

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"time"
)

const (
	recordSize         = 1024
	recordBodyEnd      = 952
	stateMagic         = "PSTATE03"
	registerMagic      = "PREG0001"
	enrolledMagic      = "PENROL01"
	panelIndexMagic    = "PINDEX01"
	nodeOwner          = "gifford"
	maxSafeInteger     = uint64(1<<53 - 1)
	maxRecordEpoch     = uint32(1<<31 - 1)
	maxRecordPane      = uint32(1<<31 - 1)
	maxRecordPID       = int64(1 << 22)
	maxEnrolledBytes   = 64 * 1024
	maxPanelIndexSize  = 1024 * 1024
	generationHeadroom = uint64(8)
	operationTransfer  = "transfer"
	operationRepair    = "repair"
	phaseIdle          = "IDLE"
	phaseRunning       = "EN_CURSO"
	phaseDone          = "HECHO"
	phaseUncertain     = "INCIERTO"
	phaseDeferred      = "APLAZADO"
	resultRepaired     = "REPARADO"
)

var (
	errInvalidRecord = errors.New("invalid pane-lock record")
	hostPattern      = regexp.MustCompile(`^[a-z0-9-]{1,63}$`)
	bootIDPattern    = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	timestampPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{9}Z$`)
)

type recordBinding struct {
	BootID    string `json:"boot_id"`
	Host      string `json:"host"`
	Pane      uint32 `json:"pane"`
	PanePID   int64  `json:"pane_pid"`
	PaneStart string `json:"pane_start"`
	SocketSHA string `json:"socket_sha"`
	UID       uint32 `json:"uid"`
}

type pendingRecord struct {
	FromEpoch  uint32 `json:"from_epoch"`
	FromGen    uint64 `json:"from_gen"`
	Operation  string `json:"op"`
	ToEpoch    uint32 `json:"to_epoch"`
	ToOwner    string `json:"to_owner"`
	ToOwnerGen uint32 `json:"to_owner_gen"`
	TxID       string `json:"txid"`
}

type registerRecord struct {
	Binding  recordBinding
	Epoch    uint32
	Owner    string
	OwnerGen uint32
	Pending  *pendingRecord
}

type registerWire struct {
	Binding      recordBinding `json:"binding"`
	Epoch        uint32        `json:"epoch"`
	Incorporated int           `json:"incorporated"`
	Owner        string        `json:"owner"`
	OwnerGen     uint32        `json:"owner_gen"`
	Pending      any           `json:"pending"`
	Version      int           `json:"v"`
}

type registerDecodeWire struct {
	Binding      recordBinding   `json:"binding"`
	Epoch        uint32          `json:"epoch"`
	Incorporated int             `json:"incorporated"`
	Owner        string          `json:"owner"`
	OwnerGen     uint32          `json:"owner_gen"`
	Pending      json.RawMessage `json:"pending"`
	Version      int             `json:"v"`
}

type stateRecord struct {
	Attempt     string `json:"attempt"`
	BootID      string `json:"boot_id"`
	ChildPID    int64  `json:"child_pid"`
	ChildStart  string `json:"child_start"`
	Epoch       uint32 `json:"epoch"`
	Gen         uint64 `json:"gen"`
	Host        string `json:"host"`
	Operation   string `json:"op"`
	Owner       string `json:"owner"`
	OwnerGen    uint32 `json:"owner_gen"`
	Pane        uint32 `json:"pane"`
	PanePID     int64  `json:"pane_pid"`
	PaneStart   string `json:"pane_start"`
	Phase       string `json:"phase"`
	Result      string `json:"result"`
	SocketSHA   string `json:"socket_sha"`
	Step        int    `json:"step"`
	Timestamp   string `json:"ts"`
	TxID        string `json:"txid"`
	UID         uint32 `json:"uid"`
	Version     int    `json:"v"`
	Writer      string `json:"writer"`
	WriterPID   int64  `json:"writer_pid"`
	WriterStart string `json:"writer_start"`
}

func (s stateRecord) binding() recordBinding {
	return recordBinding{
		BootID: s.BootID, Host: s.Host, Pane: s.Pane, PanePID: s.PanePID,
		PaneStart: s.PaneStart, SocketSHA: s.SocketSHA, UID: s.UID,
	}
}

type panelIndexEntry struct {
	Pane      uint32 `json:"pane"`
	SocketSHA string `json:"socket_sha"`
}

type enrolledIndexBody struct {
	Entries []uint32 `json:"entries"`
	Seq     uint64   `json:"seq"`
	Version int      `json:"v"`
}

type panelIndexBody struct {
	Entries []panelIndexEntry `json:"entries"`
	Seq     uint64            `json:"seq"`
	Version int               `json:"v"`
}

func encodeRegisterRecord(policy validatedRecordPolicy, record registerRecord) ([]byte, error) {
	pending := any("-")
	if record.Pending != nil {
		pending = *record.Pending
	}
	wire := registerWire{
		Binding: record.Binding, Epoch: record.Epoch, Incorporated: 1,
		Owner: record.Owner, OwnerGen: record.OwnerGen, Pending: pending, Version: 1,
	}
	if err := validateRegister(policy, record); err != nil {
		return nil, err
	}
	return encodeRecord(registerMagic, wire)
}

func decodeRegisterRecord(policy validatedRecordPolicy, raw []byte) (registerRecord, error) {
	body, err := decodeRecord(registerMagic, raw)
	if err != nil {
		return registerRecord{}, err
	}
	var wire registerDecodeWire
	if err := decodeCanonical(body, &wire); err != nil {
		return registerRecord{}, err
	}
	record := registerRecord{
		Binding: wire.Binding, Epoch: wire.Epoch, Owner: wire.Owner, OwnerGen: wire.OwnerGen,
	}
	switch {
	case bytes.Equal(wire.Pending, []byte(`"-"`)):
	case len(wire.Pending) == 0:
		return registerRecord{}, errInvalidRecord
	default:
		var pending pendingRecord
		if err := decodeCanonical(wire.Pending, &pending); err != nil {
			return registerRecord{}, err
		}
		record.Pending = &pending
	}
	if wire.Version != 1 || wire.Incorporated != 1 {
		return registerRecord{}, errInvalidRecord
	}
	if err := validateRegister(policy, record); err != nil {
		return registerRecord{}, err
	}
	return record, nil
}

func encodeStateRecord(policy validatedRecordPolicy, record stateRecord) ([]byte, error) {
	if err := validateState(policy, record); err != nil {
		return nil, err
	}
	return encodeRecord(stateMagic, record)
}

func decodeStateRecord(policy validatedRecordPolicy, raw []byte) (stateRecord, error) {
	body, err := decodeRecord(stateMagic, raw)
	if err != nil {
		return stateRecord{}, err
	}
	var record stateRecord
	if err := decodeCanonical(body, &record); err != nil {
		return stateRecord{}, err
	}
	if err := validateState(policy, record); err != nil {
		return stateRecord{}, err
	}
	return record, nil
}

func encodeRecord(magic string, value any) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode pane-lock record JSON: %w", err)
	}
	if len(magic) != 8 || len(body) > recordBodyEnd-len(magic) {
		return nil, errInvalidRecord
	}
	head := make([]byte, recordBodyEnd)
	copy(head, magic)
	copy(head[len(magic):], body)
	for index := len(magic) + len(body); index < len(head); index++ {
		head[index] = ' '
	}
	digest := sha256.Sum256(head)
	out := make([]byte, recordSize)
	copy(out, head)
	hex.Encode(out[recordBodyEnd:1016], digest[:])
	for index := 1016; index < 1023; index++ {
		out[index] = ' '
	}
	out[1023] = '\n'
	return out, nil
}

func decodeRecord(magic string, raw []byte) ([]byte, error) {
	if len(raw) != recordSize || len(magic) != 8 || string(raw[:8]) != magic ||
		!bytes.Equal(raw[1016:1023], bytes.Repeat([]byte{' '}, 7)) || raw[1023] != '\n' {
		return nil, errInvalidRecord
	}
	digest := sha256.Sum256(raw[:recordBodyEnd])
	want := make([]byte, hex.EncodedLen(len(digest)))
	hex.Encode(want, digest[:])
	if !bytes.Equal(raw[recordBodyEnd:1016], want) {
		return nil, errInvalidRecord
	}
	body := bytes.TrimRight(raw[8:recordBodyEnd], " ")
	if len(body) == 0 {
		return nil, errInvalidRecord
	}
	return bytes.Clone(body), nil
}

func decodeCanonical(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return errInvalidRecord
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errInvalidRecord
	}
	canonical, err := json.Marshal(destination)
	if err != nil || !bytes.Equal(canonical, raw) {
		return errInvalidRecord
	}
	return nil
}

func validateRegister(policy validatedRecordPolicy, record registerRecord) error {
	if !validRecordBinding(record.Binding) || !policy.ownerAllowed(record.Owner) ||
		record.Epoch > maxRecordEpoch || record.OwnerGen > maxRecordEpoch {
		return errInvalidRecord
	}
	if record.Pending == nil {
		return nil
	}
	pending := record.Pending
	if pending.Operation != operationTransfer && pending.Operation != operationRepair ||
		pending.FromEpoch > maxRecordEpoch || pending.ToEpoch > maxRecordEpoch ||
		pending.FromGen > maxSafeInteger || pending.ToOwnerGen > maxRecordEpoch ||
		!policy.ownerAllowed(pending.ToOwner) || !validHex(pending.TxID, 32) {
		return errInvalidRecord
	}
	return nil
}

func validateState(policy validatedRecordPolicy, record stateRecord) error {
	if record.Version != 3 || record.Epoch > maxRecordEpoch || record.OwnerGen > maxRecordEpoch ||
		record.Gen > maxSafeInteger || record.Step < -1 || record.Step > 4 ||
		!validToken(record.Attempt, 32) || !validToken(record.Operation, 64) ||
		!validToken(record.TxID, 32) || !policy.ownerAllowed(record.Owner) ||
		!policy.writerAllowed(record.Writer) || record.WriterPID < 0 || record.WriterPID > maxRecordPID ||
		record.ChildPID < 0 || record.ChildPID > maxRecordPID ||
		!validStart(record.WriterStart) || !validStart(record.ChildStart) ||
		!timestampPattern.MatchString(record.Timestamp) {
		return errInvalidRecord
	}
	if _, err := time.Parse("2006-01-02T15:04:05.000000000Z", record.Timestamp); err != nil {
		return errInvalidRecord
	}
	if !validRecordBinding(record.binding()) ||
		!slices.Contains([]string{phaseIdle, phaseRunning, phaseDone, phaseUncertain, phaseDeferred}, record.Phase) ||
		!slices.Contains([]string{"-", "CONFIRMADO", "SIN_EFECTO", resultRepaired}, record.Result) ||
		record.Phase == phaseRunning && record.Result != "-" {
		return errInvalidRecord
	}
	return nil
}

func validRecordBinding(binding recordBinding) bool {
	return hostPattern.MatchString(binding.Host) && bootIDPattern.MatchString(binding.BootID) &&
		binding.UID > 0 && binding.Pane <= maxRecordPane &&
		binding.PanePID > 0 && binding.PanePID <= maxRecordPID &&
		validStart(binding.PaneStart) && validHex(binding.SocketSHA, 64)
}

func validOwner(value string) bool {
	if len(value) == 0 || len(value) > 32 || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for index := 1; index < len(value); index++ {
		char := value[index]
		if char != '-' && char != '_' && (char < 'a' || char > 'z') &&
			(char < '0' || char > '9') {
			return false
		}
	}
	return true
}

func validStart(value string) bool {
	parsed, err := strconv.ParseUint(value, 10, 64)
	return err == nil && strconv.FormatUint(parsed, 10) == value
}

func validToken(value string, length int) bool {
	return value == "-" || validHex(value, length)
}

func validHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for index := range len(value) {
		char := value[index]
		if char < '0' || char > '9' {
			if char < 'a' || char > 'f' {
				return false
			}
		}
	}
	return true
}

func encodeEnrolledIndex(seq uint64, entries []uint32) ([]byte, error) {
	if seq == 0 || seq > maxSafeInteger || !slices.IsSorted(entries) || adjacentEqual(entries) {
		return nil, errInvalidRecord
	}
	if entries == nil {
		entries = []uint32{}
	}
	return encodeIndex(enrolledMagic, enrolledIndexBody{Entries: entries, Seq: seq, Version: 1})
}

func decodeEnrolledIndex(raw []byte) (uint64, []uint32, error) {
	body, err := decodeIndex(enrolledMagic, raw)
	if err != nil {
		return 0, nil, err
	}
	var index enrolledIndexBody
	if err := decodeCanonical(body, &index); err != nil || index.Entries == nil || index.Version != 1 ||
		index.Seq == 0 || index.Seq > maxSafeInteger || !slices.IsSorted(index.Entries) ||
		adjacentEqual(index.Entries) {
		return 0, nil, errInvalidRecord
	}
	return index.Seq, index.Entries, nil
}

func encodePanelIndex(seq uint64, entries []panelIndexEntry) ([]byte, error) {
	if err := validatePanelIndex(seq, entries); err != nil {
		return nil, err
	}
	if entries == nil {
		entries = []panelIndexEntry{}
	}
	return encodeIndex(panelIndexMagic, panelIndexBody{Entries: entries, Seq: seq, Version: 1})
}

func decodePanelIndex(raw []byte) (uint64, []panelIndexEntry, error) {
	body, err := decodeIndex(panelIndexMagic, raw)
	if err != nil {
		return 0, nil, err
	}
	var index panelIndexBody
	if err := decodeCanonical(body, &index); err != nil || index.Entries == nil || index.Version != 1 {
		return 0, nil, errInvalidRecord
	}
	if err := validatePanelIndex(index.Seq, index.Entries); err != nil {
		return 0, nil, err
	}
	return index.Seq, index.Entries, nil
}

func encodeIndex(magic string, body any) ([]byte, error) {
	canonical, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode pane-lock index JSON: %w", err)
	}
	head := append(append([]byte(magic+"\n"), canonical...), '\n')
	digest := sha256.Sum256(head)
	out := make([]byte, 0, len(head)+65)
	out = append(out, head...)
	out = hex.AppendEncode(out, digest[:])
	out = append(out, '\n')
	return out, nil
}

func decodeIndex(magic string, raw []byte) ([]byte, error) {
	lines := bytes.Split(raw, []byte{'\n'})
	if len(lines) != 4 || len(lines[3]) != 0 || string(lines[0]) != magic ||
		len(lines[2]) != 64 || !validHex(string(lines[2]), 64) {
		return nil, errInvalidRecord
	}
	head := append(append(append([]byte(nil), lines[0]...), '\n'), lines[1]...)
	head = append(head, '\n')
	digest := sha256.Sum256(head)
	want := make([]byte, 64)
	hex.Encode(want, digest[:])
	if !bytes.Equal(lines[2], want) {
		return nil, errInvalidRecord
	}
	return bytes.Clone(lines[1]), nil
}

func validatePanelIndex(seq uint64, entries []panelIndexEntry) error {
	if seq == 0 || seq > maxSafeInteger {
		return errInvalidRecord
	}
	for index, entry := range entries {
		if entry.Pane > maxRecordPane || !validHex(entry.SocketSHA, 64) || index > 0 &&
			!panelEntryLess(entries[index-1], entry) {
			return errInvalidRecord
		}
	}
	return nil
}

func panelEntryLess(left, right panelIndexEntry) bool {
	return left.SocketSHA < right.SocketSHA ||
		left.SocketSHA == right.SocketSHA && left.Pane < right.Pane
}

func adjacentEqual[T comparable](values []T) bool {
	for index := 1; index < len(values); index++ {
		if values[index-1] == values[index] {
			return true
		}
	}
	return false
}
