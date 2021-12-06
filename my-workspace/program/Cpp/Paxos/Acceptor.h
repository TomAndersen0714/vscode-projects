// Acceprot.h
#include <time.h>
#include <Proposer.h>
class Acceptor
{
public:
    int m_maxSerialNum;
    PROPOSAL m_lastAcceptValue;
    Acceptor(void);
    ~Acceptor(void);
    bool Propose(unsigned int serialNum, PROPOSAL &lastAcceptValue);
    bool Accept(PROPOSAL &value);
};